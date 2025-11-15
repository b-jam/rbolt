use rbolt::db::Db;
use std::path::Path;

#[test]
fn test_insert_and_get_single_key() {
    let db_path = Path::new("test_insert_single.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();
        wtxn.insert(b"hello", b"world").unwrap();
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();
        let value = rtxn.get(b"hello").unwrap();
        assert_eq!(value, Some(b"world".to_vec()));
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_insert_multiple_keys() {
    let db_path = Path::new("test_insert_multiple.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();
        wtxn.insert(b"key1", b"value1").unwrap();
        wtxn.insert(b"key2", b"value2").unwrap();
        wtxn.insert(b"key3", b"value3").unwrap();
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();
        assert_eq!(rtxn.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(rtxn.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(rtxn.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(rtxn.get(b"nonexistent").unwrap(), None);
    }

    std::fs::remove_file(db_path).unwrap();
}
