#[cfg(test)]
mod test {
    use crate::config::DEFAULT_DB_PATH;
    use crate::tree::compression::CompressionConfig;
    use crate::tree::tree_error::TreeResult;
    use crate::tree::{Tree, TreeSettings, TreeSettingsBuilder};
    use bincode::{Decode, Encode};
    use rand::prelude::*;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::mem;
    use std::path::PathBuf;
    use std::time::{Duration, Instant, SystemTime};

    #[derive(Debug, Encode, Decode, PartialEq)]
    pub struct TestStruct {
        pub a: i32,
        pub b: String,
    }

    #[derive(Debug, Encode, Decode, Clone)]
    struct User {
        user_id: u64,
        username: String,
    }

    fn clean_temp_dir() {
        let db_path = PathBuf::from(DEFAULT_DB_PATH);
        if db_path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&db_path) {
                eprintln!("Warning: failed to remove directory {:?}: {}", db_path, e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_commit_transaction_with_key_versions() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        let tx_id1 = tree.begin_transaction()?;
        tree.put_tx(tx_id1, b"key1".to_vec(), b"value1".to_vec(), None)?;
        tree.put_tx(tx_id1, b"key2".to_vec(), b"value2".to_vec(), None)?;

        let key_versions_before = {
            let tx_manager = tree.tx_manager.lock().unwrap();
            let key_versions = tx_manager.key_versions.read().unwrap();
            key_versions.clone()
        };

        assert!(key_versions_before.is_empty(), "key_versions should be empty before commit");

        tree.commit_transaction(tx_id1)?;

        let key_versions_after_first_commit = {
            let tx_manager = tree.tx_manager.lock().unwrap();
            let key_versions = tx_manager.key_versions.read().unwrap();
            key_versions.clone()
        };

        assert_eq!(key_versions_after_first_commit.len(), 2, "Should have 2 key versions after first commit");
        assert!(key_versions_after_first_commit.contains_key(&b"key1".to_vec()), "key1 should be present in key_versions");
        assert!(key_versions_after_first_commit.contains_key(&b"key2".to_vec()), "key2 should be present in key_versions");

        let key1_version_1 = key_versions_after_first_commit.get(&b"key1".to_vec()).unwrap().version;
        let key2_version_1 = key_versions_after_first_commit.get(&b"key2".to_vec()).unwrap().version;

        assert!(key1_version_1 > 0, "key1 version should be greater than 0");
        assert!(key2_version_1 > 0, "key2 version should be greater than 0");

        let tx_id2 = tree.begin_transaction()?;
        tree.put_tx(tx_id2, b"key1".to_vec(), b"updated_value1".to_vec(), None)?; // Update existing
        tree.put_tx(tx_id2, b"key3".to_vec(), b"value3".to_vec(), None)?; // Add new

        tree.commit_transaction(tx_id2)?;

        let key_versions_final = {
            let tx_manager = tree.tx_manager.lock().unwrap();
            let key_versions = tx_manager.key_versions.read().unwrap();
            key_versions.clone()
        };

        assert_eq!(key_versions_final.len(), 3, "Should have 3 key versions after second commit");
        assert!(key_versions_final.contains_key(&b"key1".to_vec()), "key1 should be present in final key_versions");
        assert!(key_versions_final.contains_key(&b"key2".to_vec()), "key2 should be present in final key_versions");
        assert!(key_versions_final.contains_key(&b"key3".to_vec()), "key3 should be present in final key_versions");

        let key1_version_2 = key_versions_final.get(&b"key1".to_vec()).unwrap().version;
        let key2_version_final = key_versions_final.get(&b"key2".to_vec()).unwrap().version;
        let key3_version_1 = key_versions_final.get(&b"key3".to_vec()).unwrap().version;

        assert!(key1_version_2 > key1_version_1, "key1 version should increase after update");
        assert_eq!(key2_version_final, key2_version_1, "key2 version should remain unchanged");
        assert!(key3_version_1 > 0, "key3 version should be greater than 0");

        let global_version = {
            let tx_manager_guard = tree.tx_manager.lock().unwrap();
            let global_version_guard = tx_manager_guard.global_version.lock().unwrap();
            *global_version_guard
        };

        let max_key_version = *[key1_version_2, key2_version_final, key3_version_1].iter().max().unwrap();
        assert_eq!(global_version, max_key_version, "Global version should match maximum key version");

        for (key, version_stamp) in key_versions_final.iter() {
            assert!(version_stamp.timestamp <= SystemTime::now(),
                    "Timestamp for key {:?} should not be in the future",
                    String::from_utf8_lossy(key));
        }

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_basic_transaction() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        let tx_id = tree.begin_transaction()?;
        tree.put_tx(tx_id, b"key1".to_vec(), b"value1".to_vec(), None)?;
        tree.put_tx(tx_id, b"key2".to_vec(), b"value2".to_vec(), None)?;
        let result1 = tree.get_tx(tx_id, b"key1")?;
        assert_eq!(result1, Some(b"value1".to_vec()));
        let result2 = tree.get_tx(tx_id, b"key2")?;
        assert_eq!(result2, Some(b"value2".to_vec()));

        let external_result = tree.get(b"key1")?;
        assert_eq!(external_result, None);

        tree.commit_transaction(tx_id)?;
        let committed_result1 = tree.get(b"key1")?;
        assert_eq!(committed_result1, Some(b"value1".to_vec()));

        let committed_result2 = tree.get(b"key2")?;
        assert_eq!(committed_result2, Some(b"value2".to_vec()));

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_transaction_rollback() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        tree.put(b"existing_key".to_vec(), b"existing_value".to_vec())?;

        let tx_id = tree.begin_transaction()?;

        tree.put_tx(tx_id, b"new_key1".to_vec(), b"new_value1".to_vec(), None)?;
        tree.put_tx(tx_id, b"new_key2".to_vec(), b"new_value2".to_vec(), None)?;
        tree.put_tx(tx_id, b"existing_key".to_vec(), b"modified_value".to_vec(), None)?;

        let tx_result = tree.get_tx(tx_id, b"existing_key")?;
        assert_eq!(tx_result, Some(b"modified_value".to_vec()));

        tree.rollback_transaction(tx_id)?;

        let result1 = tree.get(b"new_key1")?;
        assert_eq!(result1, None);
        let result2 = tree.get(b"new_key2")?;
        assert_eq!(result2, None);
        let existing_result = tree.get(b"existing_key")?;
        assert_eq!(existing_result, Some(b"existing_value".to_vec()));

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_transaction_isolation() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        tree.put(b"shared_key".to_vec(), b"original_value".to_vec())?;

        let tx1_id = tree.begin_transaction()?;
        let tx2_id = tree.begin_transaction()?;

        tree.put_tx(tx1_id, b"shared_key".to_vec(), b"tx1_value".to_vec(), None)?;
        tree.put_tx(tx1_id, b"tx1_only".to_vec(), b"tx1_data".to_vec(), None)?;

        tree.put_tx(tx2_id, b"shared_key".to_vec(), b"tx2_value".to_vec(), None)?;
        tree.put_tx(tx2_id, b"tx2_only".to_vec(), b"tx2_data".to_vec(), None)?;

        let tx1_shared = tree.get_tx(tx1_id, b"shared_key")?;
        assert_eq!(tx1_shared, Some(b"tx1_value".to_vec()));

        let tx2_shared = tree.get_tx(tx2_id, b"shared_key")?;
        assert_eq!(tx2_shared, Some(b"tx2_value".to_vec()));

        let tx1_sees_tx2 = tree.get_tx(tx1_id, b"tx2_only")?;
        assert_eq!(tx1_sees_tx2, None);

        let tx2_sees_tx1 = tree.get_tx(tx2_id, b"tx1_only")?;
        assert_eq!(tx2_sees_tx1, None);

        tree.commit_transaction(tx1_id)?;

        let global_shared = tree.get(b"shared_key")?;
        assert_eq!(global_shared, Some(b"tx1_value".to_vec()));

        let global_tx1_only = tree.get(b"tx1_only")?;
        assert_eq!(global_tx1_only, Some(b"tx1_data".to_vec()));

        let tx2_still_sees = tree.get_tx(tx2_id, b"shared_key")?;
        assert_eq!(tx2_still_sees, Some(b"tx2_value".to_vec()));

        tree.rollback_transaction(tx2_id)?;

        let final_shared = tree.get(b"shared_key")?;
        assert_eq!(final_shared, Some(b"tx1_value".to_vec()));

        let final_tx2_only = tree.get(b"tx2_only")?;
        assert_eq!(final_tx2_only, None);

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_transaction_update_existing() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        tree.put(b"update_key".to_vec(), b"original".to_vec())?;

        let tx_id = tree.begin_transaction()?;

        let original = tree.get_tx(tx_id, b"update_key")?;
        assert_eq!(original, Some(b"original".to_vec()));

        tree.put_tx(tx_id, b"update_key".to_vec(), b"updated".to_vec(), None)?;

        let updated = tree.get_tx(tx_id, b"update_key")?;
        assert_eq!(updated, Some(b"updated".to_vec()));

        let global = tree.get(b"update_key")?;
        assert_eq!(global, Some(b"original".to_vec()));

        tree.commit_transaction(tx_id)?;

        let final_global = tree.get(b"update_key")?;
        assert_eq!(final_global, Some(b"updated".to_vec()));

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_invalid_transaction_operations() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(1000)
            .build())?;

        let invalid_tx_id = 999;

        let read_result = tree.get_tx(invalid_tx_id, b"key");
        assert!(read_result.is_err());

        let write_result = tree.put_tx(invalid_tx_id, b"key".to_vec(), b"value".to_vec(), None);
        assert!(write_result.is_err());

        let commit_result = tree.commit_transaction(invalid_tx_id);
        assert!(commit_result.is_err());

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_transaction_with_ttl() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettingsBuilder::new()
            .mem_table_max_size(10)
            .build())?;

        let tx_id = tree.begin_transaction()?;

        for i in 0..12 {
            tree.put_tx(tx_id, format!("key_{}", i).as_bytes().to_vec(), format!("value_{}", i).as_bytes().to_vec(), None)?;
        }
        let ttl = Duration::from_millis(100);
        tree.put_tx(tx_id, b"ttl_key".to_vec(), b"ttl_value".to_vec(), Some(ttl))?;

        let in_tx = tree.get_tx(tx_id, b"ttl_key")?;
        assert_eq!(in_tx, Some(b"ttl_value".to_vec()));

        tree.commit_transaction(tx_id)?;

        let after_commit = tree.get(b"ttl_key")?;
        assert_eq!(after_commit, Some(b"ttl_value".to_vec()));

        std::thread::sleep(Duration::from_millis(150));

        let after_ttl = tree.get(b"ttl_key")?;
        assert_eq!(after_ttl, None);
        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_create_trees() -> TreeResult<()> {
        clean_temp_dir();

        let tree1 = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .mem_table_max_size(1000)
                .index_cache(false)
                .value_cache(false)
                .compressor(CompressionConfig::default())
                .build(),
        )?;
        let tree2 = Tree::load_with_settings(TreeSettings::default())?;
        let tree3 = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .db_path(PathBuf::from(DEFAULT_DB_PATH).join("custom_db"))
                .build(),
        )?;
        let tree4 = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .db_path(PathBuf::from(DEFAULT_DB_PATH).join("my_db"))
                .mem_table_max_size(50000)
                .build(),
        )?;
        let tree5 = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .db_path(PathBuf::from(DEFAULT_DB_PATH).join("my_db"))
                .mem_table_max_size(20000)
                .build(),
        )?;
        assert_eq!(tree1.len(), 0);
        assert_eq!(tree2.len(), 0);
        assert_eq!(tree3.len(), 0);
        assert_eq!(tree4.len(), 0);
        assert_eq!(tree5.len(), 0);

        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_write_and_load_entries_with_flush_and_random_search() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .compressor(CompressionConfig::none())
                .build(),
        )?;

        const ENTRIES: usize = 100000;
        const RANDOM_SEARCHES: usize = 5000;
        const KEY_LENGTH: usize = 16;
        const VALUE_LENGTH: usize = 100;

        println!(
            "Entries: {}, Key Length: {}, Value Length: {}",
            ENTRIES, KEY_LENGTH, VALUE_LENGTH
        );

        let mut keys = Vec::with_capacity(ENTRIES);

        let write_start = Instant::now();
        for i in 0..ENTRIES {
            let key = format!("key_{:08}_{}", i, generate_random_string(KEY_LENGTH - 12));
            let value = generate_realistic_value("user_data", VALUE_LENGTH);
            tree.put_typed(&key, &value)?;
            keys.push(key);
        }
        let write_duration = write_start.elapsed();

        let flush_start = Instant::now();
        tree.flush()?;
        let flush_duration = flush_start.elapsed();

        println!("tree.mem_table.len: {}", tree.mem_table.len());
        println!(
            "tree.immutable_mem_tables.len: {}",
            tree.immutable_mem_tables.len()
        );
        println!("tree.ss_tables.len: {}", tree.ss_tables.len());

        let mut rng = rand::rng();
        let random_keys: Vec<_> = keys.choose_multiple(&mut rng, RANDOM_SEARCHES).collect();
        let random_keys_len = random_keys.len();

        let random_read_start = Instant::now();
        let mut random_found = 0;

        for key in random_keys {
            if let Some(_value) = tree.get_typed::<String>(key)? {
                random_found += 1;
            }
        }
        let random_read_duration = random_read_start.elapsed();

        assert_eq!(
            RANDOM_SEARCHES, random_found,
            "Not all random entries found through get_typed"
        );

        println!("===> Performance statistics:");
        println!(
            "Write speed: {:.2} entries/ms",
            ENTRIES as f64 / write_duration.as_millis() as f64
        );
        println!(
            "Flush speed: {:.2} entries/ms",
            ENTRIES as f64 / flush_duration.as_millis() as f64
        );
        println!(
            "Search speed through get_typed (random): {:.2} searches/ms",
            random_keys_len as f64 / random_read_duration.as_millis() as f64
        );
        println!("{}", tree.get_index_cache_stats());
        println!("{}", tree.get_value_cache_stats());

        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_continious_write_entries_with_flush_and_random_search() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .index_cache_memory_limit(500 * 1024 * 1024)
                .value_cache_memory_limit(100 * 1024 * 1024)
                .compressor(CompressionConfig::none())
                .build(),
        )?;

        const ENTRIES: usize = 30000;
        const RANDOM_SEARCHES: usize = 1000;
        const KEY_LENGTH: usize = 16;
        const VALUE_LENGTH: usize = 100;

        println!(
            "Entries: {}, Key Length: {}, Value Length: {}",
            ENTRIES, KEY_LENGTH, VALUE_LENGTH
        );

        for i in 0..10 {
            println!("Iteration: {}", i);
            let mut keys = Vec::with_capacity(ENTRIES);

            let write_start = Instant::now();
            for i in 0..ENTRIES {
                let key = format!("key_{:08}_{}", i, generate_random_string(KEY_LENGTH - 12));
                let value = generate_realistic_value("user_data", VALUE_LENGTH);
                tree.put_typed(&key, &value)?;
                keys.push(key);
            }
            let write_duration = write_start.elapsed();

            let flush_start = Instant::now();
            tree.flush()?;
            let flush_duration = flush_start.elapsed();

            let mut rng = rand::rng();
            let random_keys: Vec<_> = keys.choose_multiple(&mut rng, RANDOM_SEARCHES).collect();
            let random_keys_len = random_keys.len();

            let random_read_start = Instant::now();
            let mut random_found = 0;

            for key in random_keys {
                if let Some(_value) = tree.get_typed::<String>(key)? {
                    random_found += 1;
                }
            }
            let random_read_duration = random_read_start.elapsed();

            assert_eq!(
                RANDOM_SEARCHES, random_found,
                "Not all random entries found through get_typed"
            );

            println!("===> Performance statistics:");
            println!(
                "Write speed: {:.2} entries/ms",
                ENTRIES as f64 / write_duration.as_millis() as f64
            );
            println!(
                "Flush speed: {:.2} entries/ms",
                ENTRIES as f64 / flush_duration.as_millis() as f64
            );
            println!(
                "Search speed through get_typed (random): {:.2} searches/ms",
                random_keys_len as f64 / random_read_duration.as_millis() as f64
            );
        }

        println!("{}", tree.get_index_cache_stats());
        println!("{}", tree.get_value_cache_stats());

        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_crash_recovery_with_wal() -> TreeResult<()> {
        clean_temp_dir();

        {
            const ENTRIES: usize = 4555;
            const KEY_LENGTH: usize = 16;
            const VALUE_LENGTH: usize = 100;

            let mut tree = Tree::load_with_settings(
                TreeSettingsBuilder::new()
                    .mem_table_max_size(2000)
                    .compressor(CompressionConfig::balanced())
                    .build(),
            )?;

            let mut keys = Vec::with_capacity(ENTRIES);
            for i in 0..ENTRIES {
                let key = format!("key_{:08}_{}", i, generate_random_string(KEY_LENGTH - 12));
                let value = generate_realistic_value("user_data", VALUE_LENGTH);
                tree.put_typed(&key, &value)?;
                keys.push(key);
            }
            tree.put(b"key1".to_vec(), b"value1".to_vec())?;
            tree.put(b"key2".to_vec(), b"value2".to_vec())?;
            mem::forget(tree);
        }

        {
            let mut recovered_tree = Tree::load_with_settings(
                TreeSettingsBuilder::new()
                    .mem_table_max_size(2000)
                    .compressor(CompressionConfig::balanced())
                    .build(),
            )?;
            let value1 = recovered_tree.get(b"key1")?;
            let value2 = recovered_tree.get(b"key2")?;
            assert_eq!(value1, Some(b"value1".to_vec()));
            assert_eq!(value2, Some(b"value2".to_vec()));
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn test_rw_100k_from_memtable() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .compressor(CompressionConfig::none())
                .index_cache(false)
                .value_cache(false)
                .mem_table_max_size(100000)
                .build(),
        )?;
        let max_entries: u64 = 99999;

        let start_time = Instant::now();
        for i in 0..=max_entries {
            let key = format!("test_key_{}", i);
            tree.put(key.as_bytes().to_vec(), key.as_bytes().to_vec())?;
        }
        let write_duration = start_time.elapsed();
        println!(
            "Write time for {} entries: {:?}",
            max_entries, write_duration
        );

        let start_time = Instant::now();
        for i in 0..=max_entries {
            let key = format!("test_key_{}", i);
            tree.get(key.as_bytes())?;
        }
        let read_duration = start_time.elapsed();

        println!("===> Performance statistics:");
        println!(
            "Write speed: {:.2} entries/ms",
            max_entries as f64 / write_duration.as_millis() as f64
        );
        println!(
            "Read speed: {:.2} entries/ms",
            max_entries as f64 / read_duration.as_millis() as f64
        );

        println!("{}", tree.get_index_cache_stats());
        println!("{}", tree.get_value_cache_stats());
        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_compression_performance() -> TreeResult<()> {
        clean_temp_dir();

        let test_data = generate_compressible_data(10000);
        let compression_configs = vec![
            CompressionConfig::fast(),
            CompressionConfig::balanced(),
            CompressionConfig::best(),
        ];

        for config in compression_configs {
            println!("Testing compression config: {:?}", config);

            let mut tree = Tree::load_with_settings(
                TreeSettingsBuilder::new()
                    .db_path(
                        PathBuf::from(DEFAULT_DB_PATH)
                            .join(format!("perf_test_{:?}", config.compression_type)),
                    )
                    .mem_table_max_size(1000)
                    .bloom_filter_error_probability(0.05)
                    .index_cache(false)
                    .value_cache(false)
                    .compressor(config)
                    .build(),
            )?;

            let start_time = Instant::now();

            for i in 0..100 {
                tree.put_typed(&format!("perf_test_{}", i), &test_data)?;
            }

            let write_time = start_time.elapsed();
            let read_start = Instant::now();

            for i in 0..100 {
                let retrieved: Option<String> = tree.get_typed(&format!("perf_test_{}", i))?;
                assert!(
                    retrieved.is_some(),
                    "Failed to retrieve data for key: perf_test_{}",
                    i
                );
                assert_eq!(
                    retrieved.unwrap(),
                    test_data,
                    "Data mismatch for key: perf_test_{}",
                    i
                );
            }

            let read_time = read_start.elapsed();

            println!("  Write time: {:?}", write_time);
            println!("  Read time: {:?}", read_time);
        }

        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_compression_with_large_objects() -> TreeResult<()> {
        clean_temp_dir();

        #[derive(Debug, Encode, Decode, Clone, PartialEq)]
        struct LargeObject {
            id: u64,
            data: Vec<String>,
            metadata: HashMap<String, String>,
        }

        let large_object = LargeObject {
            id: 12345,
            data: (0..1000).map(|i| format!("Item number {}", i)).collect(),
            metadata: {
                let mut map = HashMap::new();
                for i in 0..50 {
                    map.insert(format!("key_{}", i), format!("value_{}", i).repeat(10));
                }
                map
            },
        };

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .index_cache(false)
                .value_cache(false)
                .compressor(CompressionConfig::balanced())
                .build(),
        )?;

        tree.put_typed("large_object", &large_object)?;

        let retrieved: Option<LargeObject> = tree.get_typed("large_object")?;
        assert!(retrieved.is_some(), "Failed to retrieve large object");

        let retrieved_object = retrieved.unwrap();
        assert_eq!(retrieved_object, large_object, "Large object data mismatch");

        clean_temp_dir();

        Ok(())
    }

    #[test]
    #[serial]
    fn test_basic_string_loadtest() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .compressor(CompressionConfig::balanced())
                .build(),
        )?;

        const ENTRIES: usize = 50000;
        const KEY_LENGTH: usize = 16;
        const VALUE_LENGTH: usize = 100;

        println!("=== Basic String Load Test ===");
        println!(
            "Entries: {}, Key Length: {}, Value Length: {}",
            ENTRIES, KEY_LENGTH, VALUE_LENGTH
        );

        let write_start = Instant::now();
        let mut keys = Vec::with_capacity(ENTRIES);

        for i in 0..ENTRIES {
            let key = format!("key_{:08}_{}", i, generate_random_string(KEY_LENGTH - 12));
            let value = generate_realistic_value("user_data", VALUE_LENGTH);

            tree.put_typed(&key, &value)?;
            keys.push(key);

            if i % 10000 == 0 {
                println!("Written {} entries", i);
            }
        }

        let write_duration = write_start.elapsed();
        println!("Write phase completed in {:?}", write_duration);
        println!(
            "Write speed: {:.2} entries/sec",
            ENTRIES as f64 / write_duration.as_secs_f64()
        );

        let flush_start = Instant::now();
        tree.flush()?;
        let flush_duration = flush_start.elapsed();
        println!("Flush completed in {:?}", flush_duration);

        let read_start = Instant::now();
        let mut found_count = 0;

        for key in &keys {
            if let Some(_value) = tree.get_typed::<String>(key)? {
                found_count += 1;
            }
        }

        let read_duration = read_start.elapsed();
        println!("Sequential read completed in {:?}", read_duration);
        println!(
            "Read speed: {:.2} entries/sec",
            ENTRIES as f64 / read_duration.as_secs_f64()
        );
        println!("Found: {}/{} entries", found_count, ENTRIES);

        let mut rng = rand::rng();
        let random_keys: Vec<_> = keys.choose_multiple(&mut rng, 5000).collect();

        let random_read_start = Instant::now();
        let mut random_found = 0;

        for key in random_keys {
            if let Some(_value) = tree.get_typed::<String>(key)? {
                random_found += 1;
            }
        }

        let random_read_duration = random_read_start.elapsed();
        println!("Random read completed in {:?}", random_read_duration);
        println!(
            "Random read speed: {:.2} entries/sec",
            5000.0 / random_read_duration.as_secs_f64()
        );
        println!("Random found: {}/5000 entries", random_found);

        println!("\n=== Cache Statistics ===");
        println!("Index cache: {}", tree.get_index_cache_stats());
        println!("Value cache: {}", tree.get_value_cache_stats());

        clean_temp_dir();
        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_variable_size_loadtest() -> TreeResult<()> {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .mem_table_max_size(5000)
                .bloom_filter_cache(true)
                .compressor(CompressionConfig::fast())
                .build(),
        )?;

        println!("=== Variable Size Load Test ===");

        let test_cases = vec![
            ("small", 1000, 50, 500),     // 1000 entries, 50 byte values, 500 byte max
            ("medium", 500, 1000, 5000),  // 500 entries, 1KB values, 5KB max
            ("large", 100, 10000, 50000), // 100 entries, 10KB values, 50KB max
        ];

        for (test_name, count, min_size, max_size) in test_cases {
            println!("\n--- {} test ---", test_name);

            let write_start = Instant::now();
            let mut keys = Vec::new();

            for i in 0..count {
                let key = format!("{}_{:06}", test_name, i);
                let value_size = rand::rng().random_range(min_size..=max_size);
                let value = generate_realistic_value("log_entry", value_size);

                tree.put_typed(&key, &value)?;
                keys.push(key);
            }

            let write_duration = write_start.elapsed();
            println!(
                "Write: {} entries in {:?} ({:.2} entries/sec)",
                count,
                write_duration,
                count as f64 / write_duration.as_secs_f64()
            );
            tree.flush()?;

            let read_start = Instant::now();
            let mut found = 0;

            for key in &keys {
                if let Some(_value) = tree.get_typed::<String>(key)? {
                    found += 1;
                }
            }

            let read_duration = read_start.elapsed();
            println!(
                "Read: {}/{} entries in {:?} ({:.2} entries/sec)",
                found,
                count,
                read_duration,
                count as f64 / read_duration.as_secs_f64()
            );
        }

        println!("\n=== Final Statistics ===");
        println!("Total entries: {}", tree.len());
        println!("Index cache: {}", tree.get_index_cache_stats());
        println!("Value cache: {}", tree.get_value_cache_stats());

        Ok(())
    }

    fn generate_random_string(length: usize) -> String {
        use rand::Rng;
        let mut rng = rand::rng();
        (0..length)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect()
    }

    #[allow(dead_code)]
    fn generate_json_like_data(count: usize) -> String {
        let mut result = String::from("[");
        for i in 0..count {
            if i > 0 {
                result.push(',');
            }
            result.push_str(&format!(
                r#"{{"id": {}, "name": "user_{}", "active": {}}}"#,
                i,
                i,
                i % 2 == 0
            ));
        }
        result.push(']');
        result
    }

    fn generate_compressible_data(base_length: usize) -> String {
        let patterns = vec![
            "AAAAAAAAAA",
            "BBBBBBBBBB",
            "CCCCCCCCCC",
            "1234567890",
            "abcdefghij",
        ];

        let mut result = String::new();
        for i in 0..base_length / 10 {
            result.push_str(patterns[i % patterns.len()]);
        }
        result
    }

    fn generate_realistic_value(pattern: &str, size: usize) -> String {
        match pattern {
            "user_data" => {
                format!(
                    "{{\"id\":{},\"name\":\"{}\",\"email\":\"{}\",\"created_at\":\"{}\",\"active\":{}}}",
                    rand::rng().random_range(1..1000000),
                    generate_random_string(10),
                    generate_random_string(15),
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    rand::rng().random_bool(0.8)
                )
            }
            "log_entry" => {
                format!(
                    "[{}] {} - {} - {}",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    ["INFO", "WARN", "ERROR", "DEBUG"][rand::rng().random_range(0..4)],
                    generate_random_string(20),
                    generate_random_string(size.saturating_sub(50))
                )
            }
            "session_data" => {
                format!(
                    "session_id={}&user_id={}&data={}",
                    generate_random_string(32),
                    rand::rng().random_range(1..100000),
                    generate_random_string(size.saturating_sub(50))
                )
            }
            _ => generate_random_string(size),
        }
    }
}
