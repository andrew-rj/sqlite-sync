// The exported `#[no_mangle]` symbols here are consumed by the C linker, not
// by Rust callers — their "public" marker exists to keep the symbols visible
// to linking, not to form a Rust API. Silence the lint that would otherwise
// demand the helper types be re-exported alongside them.
#![allow(private_interfaces)]

//! HTTP transport implementations that the C side of cloudsync calls into.
//!
//! The C library defines `network_send_buffer` and `network_receive_buffer`
//! as its only externally-visible transport primitives. Building with
//! `-DCLOUDSYNC_OMIT_CURL` omits the libcurl implementations of those two
//! functions, leaving the linker looking for alternatives. We provide them
//! from Rust as `#[no_mangle] extern "C"` symbols, backed by `ureq`. Everything
//! else — endpoint resolution, auth token storage, version tracking, retry
//! loop, gap recovery, SQL function registration — still lives in C.
//!
//! See `docs/internal/network.md` in the cloudsync repo for the override
//! contract.

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::slice;
use std::time::Duration;

// Mirror network_private.h.
const CLOUDSYNC_NETWORK_OK: c_int = 1;
const CLOUDSYNC_NETWORK_ERROR: c_int = 2;
const CLOUDSYNC_NETWORK_BUFFER: c_int = 3;

const ORG_HEADER: &str = "X-CloudSync-Org";

// Matches `typedef struct { ... } NETWORK_RESULT` in network_private.h. The
// C caller either reads `buffer`/`blen` directly (and frees via the default
// allocator), or invokes `xfree(xdata)` when we set one. We always set the
// callback pair so the response body stays owned by Rust.
#[repr(C)]
struct NetworkResult {
    code: c_int,
    buffer: *mut c_char,
    blen: usize,
    xdata: *mut c_void,
    xfree: Option<unsafe extern "C" fn(*mut c_void)>,
}

impl NetworkResult {
    fn error_without_body() -> Self {
        Self {
            code: CLOUDSYNC_NETWORK_ERROR,
            buffer: ptr::null_mut(),
            blen: 0,
            xdata: ptr::null_mut(),
            xfree: None,
        }
    }
}

// Opaque handle — C owns it, we only pass it through and call the exported
// accessor for org_id. Siteid isn't needed on the transport side.
#[repr(C)]
struct NetworkData {
    _private: [u8; 0],
}

extern "C" {
    fn network_data_get_orgid(data: *mut NetworkData) -> *mut c_char;
}

// Called by the C side through `xfree` to release a Rust-allocated buffer.
unsafe extern "C" fn drop_boxed_vec(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut Vec<u8>));
    }
}

// Parse a header string of the form "Name: Value" (how the C side passes
// custom headers — same wire format curl's slist_append wants).
fn split_header(raw: &str) -> Option<(&str, &str)> {
    let mut parts = raw.splitn(2, ':');
    let name = parts.next()?.trim();
    let value = parts.next()?.trim();
    if name.is_empty() {
        None
    } else {
        Some((name, value))
    }
}

// Build a ureq agent with sensible timeouts. Re-created per call — these
// calls are infrequent (one per sync cycle) and the cost is negligible next
// to the network round-trip.
fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
}

// Safely turn a C string pointer into a &str, returning None on null or bad
// UTF-8. Bad UTF-8 in a URL/header would already be a programmer error on
// the C side, so "treat as missing" is a defensible shape.
unsafe fn cstr_opt<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        None
    } else {
        CStr::from_ptr(p).to_str().ok()
    }
}

/// Transport shim for `cloudsync`'s receive path. Mirrors the contract of
/// the libcurl implementation in network.c.
#[no_mangle]
pub unsafe extern "C" fn network_receive_buffer(
    data: *mut NetworkData,
    endpoint: *const c_char,
    authentication: *const c_char,
    zero_terminated: bool,
    is_post_request: bool,
    json_payload: *mut c_char,
    custom_header: *const c_char,
) -> NetworkResult {
    let Some(url) = cstr_opt(endpoint) else {
        return NetworkResult::error_without_body();
    };

    let has_json = !json_payload.is_null();
    let json_body = if has_json {
        match cstr_opt(json_payload) {
            Some(s) => Some(s.to_owned()),
            None => return NetworkResult::error_without_body(),
        }
    } else {
        None
    };

    let mut req = if has_json || is_post_request {
        agent().post(url)
    } else {
        agent().get(url)
    };

    if let Some(h) = cstr_opt(custom_header) {
        if let Some((name, value)) = split_header(h) {
            req = req.set(name, value);
        }
    }

    if !data.is_null() {
        let org_ptr = network_data_get_orgid(data);
        if let Some(org) = cstr_opt(org_ptr) {
            if !org.is_empty() {
                req = req.set(ORG_HEADER, org);
            }
        }
    }

    if has_json {
        req = req.set("Content-Type", "application/json");
    }

    if let Some(auth) = cstr_opt(authentication) {
        req = req.set("Authorization", &format!("Bearer {auth}"));
    }

    let response = match json_body {
        Some(body) => req.send_string(&body),
        None if is_post_request => req.send_bytes(&[]),
        None => req.call(),
    };

    let body = match response {
        Ok(resp) => read_body(resp),
        Err(ureq::Error::Status(_, resp)) => {
            // Non-2xx. Mirror the C behaviour: populate buffer with the
            // server-returned body (error payload) so the caller can surface
            // it. The caller still treats it as an error via `code`.
            let body = read_body(resp);
            return match body {
                Some(b) => build_error_with_body(b, zero_terminated),
                None => NetworkResult::error_without_body(),
            };
        }
        Err(_) => return NetworkResult::error_without_body(),
    };

    match body {
        Some(bytes) if !bytes.is_empty() => {
            build_success(bytes, zero_terminated, CLOUDSYNC_NETWORK_BUFFER)
        }
        _ => NetworkResult {
            code: CLOUDSYNC_NETWORK_OK,
            buffer: ptr::null_mut(),
            blen: 0,
            xdata: ptr::null_mut(),
            xfree: None,
        },
    }
}

/// Transport shim for `cloudsync`'s send path — an HTTP PUT of a raw blob,
/// matching what S3 presigned URLs expect. Mirrors network.c's libcurl
/// implementation.
#[no_mangle]
pub unsafe extern "C" fn network_send_buffer(
    data: *mut NetworkData,
    endpoint: *const c_char,
    authentication: *const c_char,
    blob: *const c_void,
    blob_size: c_int,
) -> bool {
    let Some(url) = cstr_opt(endpoint) else {
        return false;
    };
    if blob.is_null() || blob_size < 0 {
        return false;
    }
    let body = slice::from_raw_parts(blob as *const u8, blob_size as usize);

    let mut req = agent()
        .put(url)
        .set("Accept", "text/plain")
        .set("Content-Type", "application/octet-stream");

    if let Some(auth) = cstr_opt(authentication) {
        req = req.set("Authorization", &format!("Bearer {auth}"));
    }

    if !data.is_null() {
        let org_ptr = network_data_get_orgid(data);
        if let Some(org) = cstr_opt(org_ptr) {
            if !org.is_empty() {
                req = req.set(ORG_HEADER, org);
            }
        }
    }

    req.send_bytes(body).is_ok()
}

// Drain a ureq response into owned bytes. Returns None on I/O error.
fn read_body(resp: ureq::Response) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut buf = Vec::new();
    // Cap readable bytes to avoid a malicious/misconfigured endpoint blowing
    // us up. 64 MiB is far larger than a plausible cloudsync payload.
    const MAX: u64 = 64 * 1024 * 1024;
    let mut reader = resp.into_reader().take(MAX);
    reader.read_to_end(&mut buf).ok()?;
    Some(buf)
}

fn build_success(bytes: Vec<u8>, zero_terminated: bool, code: c_int) -> NetworkResult {
    let mut bytes = bytes;
    let blen = bytes.len();
    if zero_terminated {
        // Extra NUL that's not counted in blen. Matches the C receive-
        // callback behaviour: readers may use strlen-style scans up to the
        // NUL or bounded reads up to blen, both are valid.
        bytes.push(0);
    }
    let mut boxed = Box::new(bytes);
    // Grab the data pointer while the Box still derefs normally, then leak
    // the Box to hand the caller an opaque `xdata` for later `xfree`. The
    // Vec's allocation stays stable for the Box's lifetime, and nothing on
    // the C side should attempt to realloc or free through `buffer` directly.
    let buffer = boxed.as_mut_ptr() as *mut c_char;
    let raw_ptr = Box::into_raw(boxed);
    NetworkResult {
        code,
        buffer,
        blen,
        xdata: raw_ptr as *mut c_void,
        xfree: Some(drop_boxed_vec),
    }
}

fn build_error_with_body(bytes: Vec<u8>, zero_terminated: bool) -> NetworkResult {
    let mut result = build_success(bytes, zero_terminated, CLOUDSYNC_NETWORK_ERROR);
    result.code = CLOUDSYNC_NETWORK_ERROR;
    result
}
