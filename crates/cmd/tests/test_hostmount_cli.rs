

use std::fs::{File, create_dir_all};
use std::io::Write;
use tempfile::TempDir;

use cmd::common::{ShipContext, FilesystemChoice};
use cmd::commands::{init::init_command, mknod::mknod_command, list::list_command, cat::cat_command_with_sql};

#[tokio::test]
async fn test_hostmount_cli_end_to_end() {
    // Setup temp host directory
    let temp_host_dir = TempDir::new().expect("create temp host dir");
    let host_path = temp_host_dir.path().to_path_buf();
    create_dir_all(&host_path).expect("create host dir");

    // Create files and subdir
    let file1_path = host_path.join("file1.txt");
    let mut file1 = File::create(&file1_path).expect("create file1");
    write!(file1, "Hello from file1!").expect("write file1");

    let file2_path = host_path.join("file2.txt");
    let mut file2 = File::create(&file2_path).expect("create file2");
    write!(file2, "Hello from file2!").expect("write file2");

    let subdir_path = host_path.join("subdir");
    create_dir_all(&subdir_path).expect("create subdir");
    let nested_file_path = subdir_path.join("nested.txt");
    let mut nested_file = File::create(&nested_file_path).expect("create nested");
    write!(nested_file, "Hello from nested file!").expect("write nested");

    // Setup temp pond store
    let temp_pond = TempDir::new().expect("create temp pond");
    let pond_path = temp_pond.path().to_path_buf();
    let pond_store = pond_path.join("pond_store");
    unsafe {
        std::env::set_var("POND", &pond_store);
    }

    // Write hostmount config YAML
    let config_path = pond_path.join("hostmount.yaml");
    std::fs::write(&config_path, format!("directory: {}\n", host_path.display())).expect("write config");

    // Create CLI context
    let args: Vec<String> = vec![];
    let ship_context = ShipContext::new(None, args);

    // pond init
    assert!(init_command(&ship_context).await.is_ok());

    // pond mknod hostmount /mnt config
    let ship = ship_context.create_ship_with_transaction().await.expect("create ship with txn");
    assert!(mknod_command(ship, "hostmount", "/mnt", config_path.to_str().unwrap()).await.is_ok());

    // pond list /**
    let mut listed = Vec::new();
    list_command(&ship_context, "/**", false, FilesystemChoice::Data, |line| listed.push(line)).await.expect("run pond list");
    let stdout = listed.join("\n");
    assert!(stdout.contains("/mnt/file1.txt"));
    assert!(stdout.contains("/mnt/file2.txt"));
    assert!(stdout.contains("/mnt/subdir/nested.txt"));

    // pond cat /mnt/file1.txt and /mnt/subdir/nested.txt, capturing and validating output using string buffer
    let mut file1_content = String::new();
    cat_command_with_sql(
        &ship_context,
        "/mnt/file1.txt",
        FilesystemChoice::Data,
        "raw",
        Some(&mut file1_content),
        None,
        None,
        None,
    ).await.expect("cat file1");
    assert!(file1_content.contains("Hello from file1!"));

    let mut nested_content = String::new();
    cat_command_with_sql(
        &ship_context,
        "/mnt/subdir/nested.txt",
        FilesystemChoice::Data,
        "raw",
        Some(&mut nested_content),
        None,
        None,
        None,
    ).await.expect("cat nested");
    assert!(nested_content.contains("Hello from nested file!"));
}
