use rbolt::db::Db;
use std::path::Path;

#[test]
fn test_multiple_sequential_transactions() {
    let db_path = Path::new("test_multi_txn.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    let db = Db::open(db_path).unwrap();

    {
        let mut wtxn = db.begin_write_transaction().unwrap();
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_txn1_{}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let mut wtxn = db.begin_write_transaction().unwrap();
        for i in 50..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_txn2_{}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let mut wtxn = db.begin_write_transaction().unwrap();
        for i in 100..150 {
            let key = format!("key_{:03}", i);
            let value = format!("value_txn3_{}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let rtxn = db.begin_read_transaction().unwrap();
        for i in 0..150 {
            let key = format!("key_{:03}", i);
            let result = rtxn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist", key);
        }
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_persistence_across_reopens() {
    let db_path = Path::new("test_persistence.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 0..100 {
            let key = format!("persistent_{:03}", i);
            let value = format!("value_{}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        for i in 0..100 {
            let key = format!("persistent_{:03}", i);
            let expected_value = format!("value_{}", i);
            let result = rtxn.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(expected_value.as_bytes().to_vec()),
                      "Key {} should persist across reopens", key);
        }
    }

    {
        let db = Db::open(db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 100..150 {
            let key = format!("persistent_{:03}", i);
            let value = format!("value_{}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        for i in 0..150 {
            let key = format!("persistent_{:03}", i);
            let expected_value = format!("value_{}", i);
            let result = rtxn.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(expected_value.as_bytes().to_vec()),
                      "Key {} should persist", key);
        }
    }

    std::fs::remove_file(db_path).unwrap();
}

#[test]
fn test_read_during_write_transaction_prep() {
    let db_path = Path::new("test_concurrent.rdb");
    if db_path.exists() {
        std::fs::remove_file(db_path).unwrap();
    }

    let db = Db::open(db_path).unwrap();

    {
        let mut wtxn = db.begin_write_transaction().unwrap();
        wtxn.insert(b"key1", b"value1").unwrap();
        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    let mut wtxn = db.begin_write_transaction().unwrap();
    wtxn.insert(b"key2", b"value2").unwrap();

    let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
    db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();

    let rtxn = db.begin_read_transaction().unwrap();
    assert_eq!(rtxn.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(rtxn.get(b"key2").unwrap(), Some(b"value2".to_vec()));

    std::fs::remove_file(db_path).unwrap();
}
