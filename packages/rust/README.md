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

The cloud sync transport is always included. The C library's HTTP primitives
are satisfied from Rust using [`ureq`](https://crates.io/crates/ureq) with
rustls — no system libcurl or OpenSSL dependency, TLS handled in-process on
every platform.

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

If you'd rather build a custom transport, the raw payload functions
(`cloudsync_changes`, `cloudsync_payload_encode`/`_apply`, etc.) are still
available — just ignore `cloudsync_network_*`.

## Compatibility

Built and tested against `rusqlite` with the `bundled` feature. Any SQLite
wrapper that exposes the raw `sqlite3_auto_extension` entry point should work.

## License

MIT. See [LICENSE.md](LICENSE.md).
