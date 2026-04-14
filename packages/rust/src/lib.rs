//! Static Rust bindings for the SQLite Sync (cloudsync) extension.
//!
//! This crate compiles the cloudsync C library at build time and exposes
//! the SQLite extension entry point for use with rusqlite or any other
//! SQLite wrapper that supports `sqlite3_auto_extension`.
//!
//! # Example with rusqlite
//!
//! ```rust,no_run
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

#[link(name = "cloudsync")]
extern "C" {
    /// SQLite extension entry point for cloudsync.
    ///
    /// Register this with `sqlite3_auto_extension` to automatically load
    /// the extension into every new database connection.
    pub fn sqlite3_cloudsync_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{ffi::sqlite3_auto_extension, Connection};

    #[test]
    fn test_cloudsync_loads_and_returns_version() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(
                sqlite3_cloudsync_init as *const (),
            )));
        }

        let conn = Connection::open_in_memory().unwrap();
        let version: String = conn
            .query_row("SELECT cloudsync_version()", [], |r| r.get(0))
            .unwrap();

        assert!(
            version.starts_with("1."),
            "expected version starting with '1.', got: {version}"
        );
    }
}
