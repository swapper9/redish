#[cfg(test)]
mod test {
    use crate::config::{DEFAULT_DB_PATH, DEFAULT_MEM_TABLE_SIZE};
    use crate::tree::{Tree, TreeSettings, TreeSettingsBuilder};
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
}
