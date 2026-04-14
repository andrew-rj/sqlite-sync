use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to resolve repo root");

    let src = root.join("src");
    let sqlite_dir = root.join("sqlite");
    let fi_dir = root.join("modules/fractional-indexing");

    // Core source files (src/*.c minus postgresql-specific ones)
    let core_sources: Vec<PathBuf> = [
        "cloudsync.c",
        "block.c",
        "dbutils.c",
        "lz4.c",
        "pk.c",
        "utils.c",
    ]
    .iter()
    .map(|f| src.join(f))
    .collect();

    // SQLite-specific source files
    let sqlite_sources: Vec<PathBuf> = [
        "cloudsync_sqlite.c",
        "cloudsync_changes_sqlite.c",
        "database_sqlite.c",
        "sql_sqlite.c",
    ]
    .iter()
    .map(|f| src.join("sqlite").join(f))
    .collect();

    // Fractional indexing submodule
    let fi_source = fi_dir.join("fractional_indexing.c");

    let mut build = cc::Build::new();
    build
        .include(&src)
        .include(src.join("sqlite"))
        .include(src.join("network"))
        .include(&sqlite_dir)
        .include(&fi_dir)
        // Build as part of sqlite core (not a loadable extension)
        .define("SQLITE_CORE", None)
        // Omit network layer (curl/SSL) — users handle HTTP sync themselves
        .define("CLOUDSYNC_OMIT_NETWORK", None)
        // Suppress printf-based result printing
        .define("CLOUDSYNC_OMIT_PRINT_RESULT", None)
        .warnings(false);

    for src_file in core_sources
        .iter()
        .chain(sqlite_sources.iter())
        .chain(std::iter::once(&fi_source))
    {
        build.file(src_file);
    }

    build.compile("cloudsync");

    // Platform-specific system library linking
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" | "ios" => {
            println!("cargo:rustc-link-lib=framework=Security");
        }
        "windows" => {
            println!("cargo:rustc-link-lib=bcrypt");
        }
        _ => {} // Linux/Android use getrandom/getentropy — no extra libs needed
    }

    // Tell cargo to re-run if any C source changes
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", sqlite_dir.display());
    println!("cargo:rerun-if-changed={}", fi_dir.display());
}
