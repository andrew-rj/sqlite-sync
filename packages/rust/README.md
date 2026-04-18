# sqlite-sync

Static Rust bindings for the [SQLite Sync](https://github.com/sqliteai/sqlite-sync)
(cloudsync) extension. The C library is compiled as part of your crate build
and linked statically into your binary — no runtime shared-library loading.

## Install

```sh
cargo add sqlite-sync
cargo add rusqlite --features bundled
```

## Usage

Register the extension once before opening any connections:

```rust
use rusqlite::{ffi::sqlite3_auto_extension, Connection};
use sqlite_sync::sqlite3_cloudsync_init;

fn main() {
    unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute(
            sqlite3_cloudsync_init as *const (),
        )));
    }

    let conn = Connection::open("app.db").unwrap();
    let version: String = conn
        .query_row("SELECT cloudsync_version()", [], |r| r.get(0))
        .unwrap();
    println!("cloudsync {version}");
}
```

From that point on every `Connection` has the `cloudsync_*` SQL functions
available. See the [SQLite Sync API reference](https://github.com/sqliteai/sqlite-sync/blob/main/API.md)
for the full function catalog.

## Networking

The cloud sync transport is always included. By default the C library's HTTP
primitives are satisfied from Rust using [`ureq`](https://crates.io/crates/ureq)
with rustls — no system libcurl or OpenSSL dependency, TLS handled in-process
on every platform.

Drive sync from SQL:

```rust
conn.execute_batch("
    SELECT cloudsync_init('tasks');
    SELECT cloudsync_network_init('<managed-database-id>');
    SELECT cloudsync_network_set_apikey('<api-key>');
    SELECT cloudsync_network_sync();
")?;
```

Call `cloudsync_network_sync()` whenever you want to push local changes and
pull remote ones. Scheduling (periodic sync, sync-on-commit, sync-on-wake,
etc.) is intentionally left to the application — how you time this depends
on whether your app is a desktop background process, a CLI run once per
invocation, a server hit occasionally, or something else.

## Features

| Feature          | Default | What it does |
|------------------|---------|--------------|
| `ureq-transport` | on      | Provides the HTTP transport primitives via `ureq` + rustls. |

### Bring your own HTTP client

Disable default features to drop `ureq` and plug in your own client
(`reqwest`, `hyper`, a mock for tests, etc.):

```toml
[dependencies]
sqlite-sync = { version = "1.0", default-features = false }
```

You MUST then provide both transport primitives as `#[no_mangle] extern "C"`
symbols from somewhere in your binary. The contract matches the C header in
`src/network/network_private.h`:

```rust
#[no_mangle]
pub unsafe extern "C" fn network_send_buffer(/* ... */) -> bool {
    // PUT raw bytes to `endpoint`, return success.
}

#[no_mangle]
pub unsafe extern "C" fn network_receive_buffer(/* ... */) -> NetworkResult {
    // GET/POST JSON from `endpoint`, return NetworkResult.
}
```

If you forget, the linker fails with "undefined symbol" for those two
functions — you'll know at build time, not at the first sync call. The
default `transport.rs` in this crate is a reference implementation worth
reading for the exact signatures and buffer-ownership conventions.

If you'd rather skip the `cloudsync_network_*` SQL functions entirely, the
raw payload functions (`cloudsync_changes`, `cloudsync_payload_encode`/
`_apply`, etc.) are always available — build a custom transport on top of
those and ignore the network layer.

## Compatibility

Built and tested against `rusqlite` with the `bundled` feature. Any SQLite
wrapper that exposes the raw `sqlite3_auto_extension` entry point should work.

## License

MIT. See [LICENSE.md](LICENSE.md).
