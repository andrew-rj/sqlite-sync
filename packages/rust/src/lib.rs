//! Static Rust bindings for the [SQLite Sync] (cloudsync) extension.
//!
//! This crate compiles the cloudsync C library at build time and exposes the
//! SQLite extension entry point for use with [`rusqlite`] (or any SQLite
//! wrapper that accepts a function pointer through `sqlite3_auto_extension`).
//!
//! # Example with rusqlite
//!
//! ```no_run
//! use rusqlite::{ffi::sqlite3_auto_extension, Connection};
//! use sqlite_sync::sqlite3_cloudsync_init;
//!
//! unsafe {
//!     sqlite3_auto_extension(Some(std::mem::transmute(
//!         sqlite3_cloudsync_init as *const (),
//!     )));
//! }
//!
//! let conn = Connection::open_in_memory().unwrap();
//! let version: String = conn
//!     .query_row("SELECT cloudsync_version()", [], |r| r.get(0))
//!     .unwrap();
//! println!("cloudsync version: {version}");
//! ```
//!
//! # Networking
//!
//! The cloud sync transport is always included: the `cloudsync_network_init`,
//! `cloudsync_network_set_apikey`, `cloudsync_network_sync` (and related) SQL
//! functions are registered, and the C layer calls two Rust-provided
//! primitives — `network_send_buffer` and `network_receive_buffer` — for
//! every HTTP request.
//!
//! # Features
//!
//! * `ureq-transport` *(default)* — supplies the two transport primitives via
//!   [`ureq`] + rustls. No system libcurl or OpenSSL is required.
//!
//! Disable default features to bring your own HTTP client. You MUST then
//! provide both primitives as `#[no_mangle] extern "C"` symbols with the
//! signatures documented in `docs/internal/network.md`:
//!
//! ```ignore
//! #[no_mangle]
//! pub unsafe extern "C" fn network_send_buffer(/* ... */) -> bool { /* ... */ }
//!
//! #[no_mangle]
//! pub unsafe extern "C" fn network_receive_buffer(/* ... */) -> NetworkResult { /* ... */ }
//! ```
//!
//! Missing symbols surface as a link-time error, so forgetting this is caught
//! at build time rather than at runtime.
//!
//! [SQLite Sync]: https://github.com/sqliteai/sqlite-sync
//! [`rusqlite`]: https://crates.io/crates/rusqlite
//! [`ureq`]: https://crates.io/crates/ureq

#[cfg(feature = "ureq-transport")]
mod transport;

#[link(name = "cloudsync")]
extern "C" {
    /// SQLite extension entry point for cloudsync.
    ///
    /// Register this with `sqlite3_auto_extension` to automatically load the
    /// extension into every new database connection. The true C signature is
    /// `int(sqlite3*, char**, const sqlite3_api_routines*)`; SQLite only calls
    /// it through a function pointer so the FFI declaration can be opaque.
    pub fn sqlite3_cloudsync_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{ffi::sqlite3_auto_extension, Connection};

    fn register_once() {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(
                sqlite3_cloudsync_init as *const (),
            )));
        });
    }

    #[test]
    fn returns_version() {
        register_once();
        let conn = Connection::open_in_memory().unwrap();
        let version: String = conn
            .query_row("SELECT cloudsync_version()", [], |r| r.get(0))
            .unwrap();
        assert!(
            version.starts_with("1."),
            "expected a 1.x version, got: {version}"
        );
    }

    // The C side registers the cloudsync_network_* SQL functions and resolves
    // its HTTP primitives against this crate's Rust transport. We can't hit
    // the real cloud from a unit test, but we *can* confirm the function is
    // registered.
    #[test]
    fn network_functions_are_registered() {
        register_once();
        let conn = Connection::open_in_memory().unwrap();
        let registered: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_function_list
                 WHERE name = 'cloudsync_network_init'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(registered, 1, "cloudsync_network_init should be registered");
    }
}
