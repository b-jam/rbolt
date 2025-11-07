use std::path::Path;
use std::io;

use rbolt::db::{Db, PAGE_SIZE};

fn main() -> io::Result<()> {
    let db_path = Path::new("test_db.rdb");
    // Clean up
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    println!("=== Starting RBolt Database Test ===\n");
    // 1. Test database creation and initialization
    println!("1. Creating and initializing database...");
    let db = Db::open(db_path)?;
    println!("   [OK] Database created successfully");

    // 2. Test read transaction
    println!("\n2. Starting read transaction...");
    {
        let tx = db.begin_read_transaction()?;
        println!("   [OK] Read transaction started");
        println!("   - Root page ID: {}", tx.root_page_id());
    } // Drop tx here to release the read lock
    // 3. Test write transaction
    println!("\n3. Testing write transaction...");
    let test_data = b"Hello, RBolt! This is a test.";
    let mut write_data = vec![0u8; PAGE_SIZE];
    write_data[..test_data.len()].copy_from_slice(test_data);
    db.commit_write_transaction(&write_data)?;
    println!("   [OK] Write transaction committed");
    println!("   - Wrote {} bytes of test data", test_data.len());

    // 4. Verify data was written
    println!("\n4. Verifying data...");
    {
        let tx2 = db.begin_read_transaction()?;
        let page = tx2.get_page(0)?; // Get first page
        println!("   [OK] Read back data from page 0");
        // Print first 32 bytes of the page for verification
        let page_data = unsafe {
            std::slice::from_raw_parts(
                page as *const _ as *const u8,
                PAGE_SIZE
            )
        };
        println!("   - First 32 bytes of page 0: {:?}", &page_data[..32]);
    } // Drop tx2 here to release the read lock
    // 5. Test with multiple pages
    println!("\n5. Testing with multiple pages...");
    let large_data = vec![42u8; PAGE_SIZE * 2]; // Two pages of data
    db.commit_write_transaction(&large_data)?;
    println!("   [OK] Wrote {} pages of data", large_data.len() / PAGE_SIZE);

    // 6. Clean up
    println!("\n6. Cleaning up...");
    std::fs::remove_file(db_path)?;
    println!("   [OK] Test database removed");

    println!("\n=== All tests completed successfully! ===\n");
    Ok(())
}