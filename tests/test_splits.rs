use rbolt::db::Db;
use std::path::Path;

#[test]
fn test_page_split() {
    let db_path = Path::new("test_split.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 0..200 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        for i in 0..200 {
            let key = format!("key_{:04}", i);
            let expected_value = format!("value_{:04}", i);
            let result = rtxn.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(expected_value.as_bytes().to_vec()),
                      "Key {} mismatch", key);
        }
    }

    std::fs::remove_file(db_path).unwrap();
}
