use rbolt::db::Db;
use std::path::Path;

#[test]
fn test_duplicate_key_overwrites() {
    let db_path = Path::new("test_duplicates.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        wtxn.insert(b"mykey", b"value1").unwrap();
        wtxn.insert(b"mykey", b"value2").unwrap();
        wtxn.insert(b"mykey", b"value3").unwrap();

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        let result = rtxn.get(b"mykey").unwrap();
        assert_eq!(result, Some(b"value3".to_vec()), "Should have last inserted value");
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_empty_key_and_value() {
    let db_path = Path::new("test_empty.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        wtxn.insert(b"", b"").unwrap();
        wtxn.insert(b"nonempty", b"").unwrap();
        wtxn.insert(b"", b"nonempty").unwrap();

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        assert_eq!(rtxn.get(b"").unwrap(), Some(b"nonempty".to_vec()));
        assert_eq!(rtxn.get(b"nonempty").unwrap(), Some(b"".to_vec()));
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_large_keys_and_values() {
    let db_path = Path::new("test_large.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        let large_key = vec![b'k'; 1024];
        let large_value = vec![b'v'; 2048];

        wtxn.insert(&large_key, &large_value).unwrap();

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        let large_key = vec![b'k'; 1024];
        let expected_value = vec![b'v'; 2048];

        let result = rtxn.get(&large_key).unwrap();
        assert_eq!(result, Some(expected_value));
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_query_empty_database() {
    let db_path = Path::new("test_empty_db.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        assert_eq!(rtxn.get(b"anykey").unwrap(), None);
    }

    std::fs::remove_file(db_path).unwrap();
}
