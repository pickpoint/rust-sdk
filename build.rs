fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prefer regenerating from the sibling pickpoint-proto checkout when present.
    // Published crates ship committed stubs under src/tracking/v2/.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto_root = manifest_dir.join("../pickpoint-proto");
    if !proto_root.join("tracking/v2/messages.proto").exists() {
        println!("cargo:warning=pickpoint-proto not found; using committed tracking stubs");
        return Ok(());
    }

    let messages = proto_root.join("tracking/v2/messages.proto");
    let service = proto_root.join("tracking/v2/service.proto");
    println!("cargo:rerun-if-changed={}", messages.display());
    println!("cargo:rerun-if-changed={}", service.display());

    let out_dir = manifest_dir.join("src/tracking/v2");
    std::fs::create_dir_all(&out_dir)?;

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(&[messages, service], &[&proto_root])?;

    Ok(())
}
