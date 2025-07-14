#[cfg(test)]
mod test {
    use crate::config::{DEFAULT_DB_PATH, DEFAULT_MEM_TABLE_SIZE};
    use crate::tree::compression::{CompressionConfig, Compressor};
    use crate::tree::{CompressionType, Tree, TreeSettings, TreeSettingsBuilder};
    use bincode::{Decode, Encode};
    use serial_test::serial;
    use std::path::PathBuf;

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
    fn test_create_trees() {
        clean_temp_dir();

        let tree1 = Tree::load_with_settings(TreeSettings {
            db_path: Default::default(),
            bincode_config: Default::default(),
            mem_table_max_size: 1000,
            enable_index_cache: false,
            enable_value_cache: false,
            compressor: Compressor::new(CompressionConfig::default()),
        });
        let tree2 = Tree::load_with_settings(TreeSettings::default());
        let tree3 = Tree::load_with_settings(
            TreeSettings::default().with_db_path(PathBuf::from(DEFAULT_DB_PATH).join("custom_db"))
        );
        let tree4 = Tree::load_with_settings(
            TreeSettings::new()
                .with_db_path(PathBuf::from(DEFAULT_DB_PATH).join("my_db"))
                .with_mem_table_max_size(50000)
        );
        let tree5 = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .db_path(PathBuf::from(DEFAULT_DB_PATH).join("my_db"))
                .mem_table_max_size(20000)
                .build()
        );
        assert_eq!(tree1.len(), 0);
        assert_eq!(tree2.len(), 0);
        assert_eq!(tree3.len(), 0);
        assert_eq!(tree4.len(), 0);
        assert_eq!(tree5.len(), 0);

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_put_entries_with_merge_ss_tables() {
        clean_temp_dir();

        let mut tree = Tree::load_with_path(DEFAULT_DB_PATH);
        let max_entries: u64 = 100000;
        for i in 1..=max_entries {
            let user = User {
                user_id: i,
                username: format!("test_user_{}", i),
            };
            tree.put_typed::<User>(&format!("test_user_{}", i), &user);
        }
        tree.flush();
        assert!((tree.ss_tables.len() as u64) < (max_entries / DEFAULT_MEM_TABLE_SIZE as u64 / 3));
        assert_eq!(tree.len(), max_entries as usize);

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_load_entries_with_flush_and_index_search() {
        clean_temp_dir();

        let mut tree = Tree::load_with_path(DEFAULT_DB_PATH);
        let max_entries: u64 = 100000;

        let start_time = std::time::Instant::now();

        for i in 1..=max_entries {
            let user = User {
                user_id: i,
                username: format!("flush_test_user_{}", i),
            };
            tree.put_typed::<User>(&format!("flush_test_user_{}", i), &user);
        }

        let write_duration = start_time.elapsed();
        println!("Write time for {} entries: {:?}", max_entries, write_duration);

        println!("===> Tree state BEFORE flush:");
        println!("tree.mem_table.len: {}", tree.mem_table.len());
        println!("tree.immutable_mem_tables.len: {}", tree.immutable_mem_tables.len());
        println!("tree.ss_tables.len: {}", tree.ss_tables.len());
        let flush_start = std::time::Instant::now();
        tree.flush();
        let flush_duration = flush_start.elapsed();
        println!("Flush time: {:?}", flush_duration);

        println!("===> Tree state AFTER flush:");
        println!("tree.mem_table.len: {}", tree.mem_table.len());
        println!("tree.immutable_mem_tables.len: {}", tree.immutable_mem_tables.len());
        println!("tree.ss_tables.len: {}", tree.ss_tables.len());

        use rand::Rng;
        let mut rng = rand::rng();
        let random_indices: Vec<u64> = (0..1000)
            .map(|_| rng.random_range(1..=max_entries))
            .collect();

        let normal_search_start = std::time::Instant::now();
        let mut found_normal = 0;

        for &index in &random_indices {
            let key = format!("flush_test_user_{}", index);
            if tree.get_typed::<User>(key.as_str()).is_some() {
                found_normal += 1;
            }
        }

        let normal_search_duration = normal_search_start.elapsed();
        println!("Search time for {} random entries through get_typed: {:?}",
                 random_indices.len(), normal_search_duration);
        println!("Found through get_typed: {}/{}", found_normal, random_indices.len());

        let test_indices = [1, 1000, 5000, 7500, 10000];
        for &index in &test_indices {
            let key = format!("flush_test_user_{}", index);

            let user = tree.get_typed::<User>(key.as_str());
            assert!(user.is_some(), "User {} not found", key);

            let user_data = user.unwrap();
            assert_eq!(user_data.user_id, index);
            assert_eq!(user_data.username, format!("flush_test_user_{}", index));

            println!("Checked user: user_id={}, username={}",
                     user_data.user_id, user_data.username);
        }

        assert_eq!(found_normal, random_indices.len(), "Not all random entries found through get_typed");

        println!("===> Performance statistics:");
        println!("Write speed: {:.2} entries/ms",
                 max_entries as f64 / write_duration.as_millis() as f64);
        println!("Flush speed: {:.2} entries/ms",
                 max_entries as f64 / flush_duration.as_millis() as f64);
        println!("Search speed through get_typed (random): {:.2} searches/ms",
                 random_indices.len() as f64 / normal_search_duration.as_millis() as f64);
        println!("{:?}", tree.get_index_cache_stats());
        println!("{:?}", tree.get_value_cache_stats());
        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_functionality() {
        clean_temp_dir();

        let compression_types = vec![
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ];

        for compression_type in compression_types {
            println!("Testing compression type: {:?}", compression_type);

            let config = CompressionConfig::new(compression_type);
            let mut tree = Tree::load_with_settings(TreeSettings {
                db_path: PathBuf::from(DEFAULT_DB_PATH).join(format!("compression_test_{:?}", compression_type)),
                bincode_config: Default::default(),
                mem_table_max_size: 1000,
                enable_index_cache: false,
                enable_value_cache: false,
                compressor: Compressor::new(config),
            });

            let test_cases = vec![
                ("repeating_data", "A".repeat(1000)),
                ("random_data", generate_random_string(1000)),
                ("json_like", generate_json_like_data(100)),
                ("mixed_data", format!("{}{}{}", "A".repeat(300), generate_random_string(300), "B".repeat(400))),
            ];

            for (test_name, test_data) in test_cases {
                let key = format!("{}_{}", test_name, compression_type as u8);
                let original_size = test_data.len();

                tree.put_typed(&key, &test_data);

                let retrieved_data: Option<String> = tree.get_typed(&key);
                assert!(retrieved_data.is_some(), "Failed to retrieve data for key: {}", key);

                let retrieved = retrieved_data.unwrap();
                assert_eq!(retrieved, test_data, "Data mismatch for key: {}", key);

                println!("  {}: original size = {}, compression type = {:?}",
                         test_name, original_size, compression_type);
            }

            let stats = tree.get_compression_stats();
            println!("  Compression stats: {:?}", stats);

            if compression_type != CompressionType::None {
                assert!(stats.compression_operations > 0, "No compression operations recorded");
                assert!(stats.decompression_operations > 0, "No decompression operations recorded");
            }
        }

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_performance() {
        clean_temp_dir();

        let test_data = generate_compressible_data(10000);
        let compression_configs = vec![
            CompressionConfig::fast(),
            CompressionConfig::balanced(),
            CompressionConfig::best(),
        ];

        for config in compression_configs {
            println!("Testing compression config: {:?}", config);

            let mut tree = Tree::load_with_settings(TreeSettings {
                db_path: PathBuf::from(DEFAULT_DB_PATH).join(format!("perf_test_{:?}", config.compression_type)),
                bincode_config: Default::default(),
                mem_table_max_size: 1000,
                enable_index_cache: false,
                enable_value_cache: false,
                compressor: Compressor::new(config),
            });

            let start_time = std::time::Instant::now();

            for i in 0..100 {
                tree.put_typed(&format!("perf_test_{}", i), &test_data);
            }

            let write_time = start_time.elapsed();
            let read_start = std::time::Instant::now();

            for i in 0..100 {
                let retrieved: Option<String> = tree.get_typed(&format!("perf_test_{}", i));
                assert!(retrieved.is_some(), "Failed to retrieve data for key: perf_test_{}", i);
                assert_eq!(retrieved.unwrap(), test_data, "Data mismatch for key: perf_test_{}", i);
            }

            let read_time = read_start.elapsed();

            let stats = tree.get_compression_stats();
            println!("  Write time: {:?}", write_time);
            println!("  Read time: {:?}", read_time);
            println!("  Compression ratio: {:.2}%", stats.compression_ratio_percentage());
            println!("  Average compression time: {:.2}ms", stats.average_compression_time_ms());
            println!("  Average decompression time: {:.2}ms", stats.average_decompression_time_ms());
        }

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_with_large_objects() {
        clean_temp_dir();

        #[derive(Debug, Encode, Decode, Clone, PartialEq)]
        struct LargeObject {
            id: u64,
            data: Vec<String>,
            metadata: std::collections::HashMap<String, String>,
        }

        let large_object = LargeObject {
            id: 12345,
            data: (0..1000).map(|i| format!("Item number {}", i)).collect(),
            metadata: {
                let mut map = std::collections::HashMap::new();
                for i in 0..50 {
                    map.insert(format!("key_{}", i), format!("value_{}", i).repeat(10));
                }
                map
            },
        };

        let mut tree = Tree::load_with_settings(TreeSettings {
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            bincode_config: Default::default(),
            mem_table_max_size: 10000,
            enable_index_cache: false,
            enable_value_cache: false,
            compressor: Compressor::new(CompressionConfig::balanced()),
        });

        tree.put_typed("large_object", &large_object);

        let retrieved: Option<LargeObject> = tree.get_typed("large_object");
        assert!(retrieved.is_some(), "Failed to retrieve large object");

        let retrieved_object = retrieved.unwrap();
        assert_eq!(retrieved_object, large_object, "Large object data mismatch");

        let stats = tree.get_compression_stats();
        println!("Large object compression stats: {:?}", stats);
        assert!(stats.compression_operations > 0, "No compression operations for large object");
        assert!(stats.decompression_operations > 0, "No decompression operations for large object");

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_error_handling() {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettings {
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            bincode_config: Default::default(),
            mem_table_max_size: 10000,
            enable_index_cache: false,
            enable_value_cache: false,
            compressor: Compressor::new(CompressionConfig::balanced()),
        });

        let test_cases = vec![
            ("empty", String::new()),
            ("small", "small".to_string()),
            ("medium", "medium".repeat(100)),
            ("large", "large".repeat(10000)),
        ];

        for (name, data) in test_cases {
            tree.put_typed(name, &data);

            let retrieved: Option<String> = tree.get_typed(name);
            assert!(retrieved.is_some(), "Failed to retrieve data for: {}", name);
            assert_eq!(retrieved.unwrap(), data, "Data mismatch for: {}", name);
        }

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_ratio_analysis() {
        clean_temp_dir();

        let mut tree = Tree::load_with_settings(TreeSettings {
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            bincode_config: Default::default(),
            mem_table_max_size: 10000,
            enable_index_cache: false,
            enable_value_cache: false,
            compressor: Compressor::new(CompressionConfig::balanced()),
        });

        let test_cases = vec![
            ("highly_compressible", "ABCDEFGHIJ".repeat(1000)),
            ("medium_compressible", generate_json_like_data(500)),
            ("low_compressible", generate_random_string(5000)),
        ];

        for (name, data) in test_cases {
            tree.reset_compression_stats();

            tree.put_typed(name, &data);
            let _retrieved: Option<String> = tree.get_typed(name);

            let stats = tree.get_compression_stats();
            println!("Data type: {}", name);
            println!("  Original size estimate: {}", data.len());
            println!("  Compression ratio: {:.2}%", stats.compression_ratio_percentage());
            println!("  Compression time: {:.2}ms", stats.average_compression_time_ms());
            println!("  Decompression time: {:.2}ms", stats.average_decompression_time_ms());
            println!("  Operations: {} compress, {} decompress",
                     stats.compression_operations, stats.decompression_operations);
            println!();
        }

        clean_temp_dir();
    }

    #[test]
    #[serial]
    fn test_compression_types_comparison() {
        clean_temp_dir();

        let test_data = generate_compressible_data(5000);
        let compression_types = vec![
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ];

        println!("Comparing compression types with {} bytes of data:", test_data.len());

        for compression_type in compression_types {
            let config = CompressionConfig::new(compression_type);
            let mut tree = Tree::load_with_settings(TreeSettings {
                db_path: PathBuf::from(DEFAULT_DB_PATH).join(format!("comparison_{:?}", compression_type)),
                bincode_config: Default::default(),
                mem_table_max_size: 1000,
                enable_index_cache: false,
                enable_value_cache: false,
                compressor: Compressor::new(config),
            });

            let start_time = std::time::Instant::now();

            for i in 0..10 {
                tree.put_typed(&format!("test_{}", i), &test_data);
            }

            let write_time = start_time.elapsed();

            let read_start = std::time::Instant::now();

            for i in 0..10 {
                let _retrieved: Option<String> = tree.get_typed(&format!("test_{}", i));
            }

            let read_time = read_start.elapsed();
            let stats = tree.get_compression_stats();

            println!("  {:?}:", compression_type);
            println!("    Write time: {:?}", write_time);
            println!("    Read time: {:?}", read_time);
            if compression_type != CompressionType::None {
                println!("    Compression ratio: {:.2}%", stats.compression_ratio_percentage());
                println!("    Avg compression time: {:.2}ms", stats.average_compression_time_ms());
                println!("    Avg decompression time: {:.2}ms", stats.average_decompression_time_ms());
            }
            println!();
        }

        clean_temp_dir();
    }

    fn generate_random_string(length: usize) -> String {
        use rand::Rng;
        let mut rng = rand::rng();
        (0..length)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect()
    }

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

}
