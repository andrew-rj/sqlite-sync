use std::path::{Path, PathBuf};

// Core C sources (src/*.c). Mirrors Makefile's CORE_SRC minus PostgreSQL.
const CORE_SOURCES: &[&str] = &[
    "src/cloudsync.c",
    "src/block.c",
    "src/dbutils.c",
    "src/lz4.c",
    "src/pk.c",
    "src/utils.c",
];

// SQLite-backend sources (src/sqlite/*.c). Mirrors Makefile's SQLITE_SRC.
const SQLITE_SOURCES: &[&str] = &[
    "src/sqlite/cloudsync_sqlite.c",
    "src/sqlite/cloudsync_changes_sqlite.c",
    "src/sqlite/database_sqlite.c",
    "src/sqlite/sql_sqlite.c",
];

// Fractional indexing submodule.
const FI_SOURCES: &[&str] = &["modules/fractional-indexing/fractional_indexing.c"];

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Two source layouts are supported:
    //   1. In-tree development: the crate lives at packages/rust/ inside the
    //      monorepo, so the repo root is two levels up.
    //   2. Published crate: `make rust-vendor` has copied the relevant trees
    //      under `csrc/`, and that's all crates.io sees.
    let root = resolve_source_root(&manifest_dir);

    let network_enabled = cfg!(feature = "network");

    let mut build = cc::Build::new();
    build
        .include(root.join("src"))
        .include(root.join("src/sqlite"))
        .include(root.join("src/network"))
        .include(root.join("sqlite"))
        .include(root.join("modules/fractional-indexing"))
        // Statically compiled into the host binary: bypass the loadable-
        // extension indirection and call sqlite3 symbols directly. Matches
        // sqlite-vec's approach.
        .define("SQLITE_CORE", None)
        // Silence the debug printf-style helpers.
        .define("CLOUDSYNC_OMIT_PRINT_RESULT", None)
        .warnings(false);

    if network_enabled {
        // Keep network.c in the build so the C SQL glue (cloudsync_network_*)
        // registers, but skip the libcurl-specific transport — Rust supplies
        // `network_send_buffer` and `network_receive_buffer` at link time.
        build.define("CLOUDSYNC_OMIT_CURL", None);
    } else {
        // Drop the whole network layer; the SQL surface loses the
        // cloudsync_network_* functions and pk.c / utils.c / etc. stand alone.
        build.define("CLOUDSYNC_OMIT_NETWORK", None);
    }

    let mut sources: Vec<&&str> = CORE_SOURCES
        .iter()
        .chain(SQLITE_SOURCES.iter())
        .chain(FI_SOURCES.iter())
        .collect();
    let network_c = "src/network/network.c";
    if network_enabled {
        sources.push(&network_c);
    }

    for src in sources {
        let path = root.join(src);
        build.file(&path);
        println!("cargo:rerun-if-changed={}", path.display());
    }

    build.compile("cloudsync");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" | "ios" => println!("cargo:rustc-link-lib=framework=Security"),
        "windows" => println!("cargo:rustc-link-lib=bcrypt"),
        _ => {}
    }

    println!("cargo:rerun-if-changed=build.rs");
}

fn resolve_source_root(manifest_dir: &Path) -> PathBuf {
    let in_tree = manifest_dir.join("../..");
    if in_tree.join("src/cloudsync.c").is_file() {
        return in_tree
            .canonicalize()
            .expect("failed to canonicalize monorepo root");
    }

    let vendored = manifest_dir.join("csrc");
    if vendored.join("src/cloudsync.c").is_file() {
        return vendored;
    }

    panic!(
        "sqlite-sync: no C sources found. Expected either a monorepo checkout \
         (../../src/cloudsync.c) or vendored sources (csrc/src/cloudsync.c). \
         If building from the repo, run `make rust-vendor` first."
    );
}
