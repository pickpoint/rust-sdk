fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prefer regenerating from the sibling pickpoint-proto checkout when present.
    // Published crates ship committed stubs under src/tracking/v2/.
    let proto_root = std::path::Path::new("../pickpoint-proto");
    if !proto_root.join("tracking/v2/messages.proto").exists() {
        println!("cargo:warning=pickpoint-proto not found; using committed tracking stubs");
        return Ok(());
    }

    println!("cargo:rerun-if-changed=../pickpoint-proto/tracking/v2/messages.proto");
    println!("cargo:rerun-if-changed=../pickpoint-proto/tracking/v2/service.proto");

    std::fs::create_dir_all("src/tracking/v2")?;

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/tracking/v2")
        .compile_protos(
            &[
                "../pickpoint-proto/tracking/v2/messages.proto",
                "../pickpoint-proto/tracking/v2/service.proto",
            ],
            &["../pickpoint-proto"],
        )?;

    Ok(())
}
