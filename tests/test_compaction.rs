use rbolt::db::Db;
use std::path::Path;

#[test]
fn test_compaction_triggered() {
    let db_path = Path::new("test_compaction.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path).ok();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 0..30 {
            let key = format!("key{:03}", i);
            let value = format!("large_value_{:03}_padding_to_make_it_big", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in (0..30).step_by(2) {
            let key = format!("key{:03}", i);
            let value = "x";
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 30..50 {
            let key = format!("key{:03}", i);
            let value = format!("value_{:03}", i);
            wtxn.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        for i in (0..30).step_by(2) {
            let key = format!("key{:03}", i);
            let value = rtxn.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(b"x".to_vec()), "Key {} should have been updated", key);
        }

        for i in (1..30).step_by(2) {
            let key = format!("key{:03}", i);
            let value = rtxn.get(key.as_bytes()).unwrap();
            assert!(value.is_some());
            let val_str = String::from_utf8(value.unwrap()).unwrap();
            assert!(val_str.contains("large_value"), "Key {} should have original value", key);
        }

        for i in 30..50 {
            let key = format!("key{:03}", i);
            let value = rtxn.get(key.as_bytes()).unwrap();
            assert_eq!(
                value,
                Some(format!("value_{:03}", i).as_bytes().to_vec()),
                "Key {} should exist",
                key
            );
        }
    }

    std::fs::remove_file(&db_path).ok();
}

#[test]
fn test_compaction_vs_split() {
    let db_path = Path::new("test_compact_vs_split.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path).ok();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 0..100 {
            let key = format!("k{:04}", i);
            let value = vec![b'x'; 30];
            wtxn.insert(key.as_bytes(), &value).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in (0..100).step_by(3) {
            let key = format!("k{:04}", i);
            let value = b"y";
            wtxn.insert(key.as_bytes(), value).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let mut wtxn = db.begin_write_transaction().unwrap();

        for i in 100..120 {
            let key = format!("k{:04}", i);
            let value = vec![b'z'; 30];
            wtxn.insert(key.as_bytes(), &value).unwrap();
        }

        let (dirty_pages, highest_page_id, root_page_id) = wtxn.prepare_commit();
        db.commit(dirty_pages, highest_page_id, root_page_id).unwrap();
    }

    {
        let db = Db::open(&db_path).unwrap();
        let rtxn = db.begin_read_transaction().unwrap();

        for i in 0..120 {
            let key = format!("k{:04}", i);
            let value = rtxn.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should exist", key);
        }
    }

    std::fs::remove_file(&db_path).ok();
}
