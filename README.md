# Redish

A lightweight in-memory key-value storage DB created based on LSM Tree.

## Features

- Lightweight LSM tree based key-value storage 
- TTL (Time-to-Live) support for keys
- Persistence with sstable auto-merge
- Optional index caching
- Optional value caching
- Optional compression (lz4, zstd or snappy)

## Usage:

```
use std::time::Duration;
use redish::tree::Tree;
use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode, Clone)]
struct User {
    user_id: u64,
    username: String,
}
let user = User {user_id: 3, username: "JohnDoe2020".to_string()};

// to use default path for db 
let mut tree = Tree::load();

// or specified path
let mut tree2 = Tree::load_with_path("/path/to/db/with_file_name");


tree.put("key1".to_string().into_bytes(), "value".to_string().into_bytes());
tree.put_with_ttl("key2".to_string().into_bytes(), "value".to_string().into_bytes(), Some(Duration::from_secs(60)));
tree.put_typed::<User>("key3", &user);

assert_eq!(user.username, tree.get_typed::<User>("key3"));
```
### Creating TreeSettings for Tree:
```
let tree = Tree::load_with_settings(TreeSettings {
            db_path: Default::default(),
            bincode_config: Default::default(),
            mem_table_max_size: 1000,
            enable_index_cache: true,
            enable_value_cache: true,
            compressor: Compressor::new(CompressionConfig::balanced()),
        });
let tree = Tree::load_with_settings(TreeSettings::default());
let tree = Tree::load_with_settings(
            TreeSettings::default().with_db_path("./custom_db")
        );
let tree = Tree::load_with_settings(
            TreeSettings::new()
                .with_db_path("./my_db")
                .with_mem_table_max_size(50000)
                .build()
        );
let tree = Tree::load_with_settings(
            TreeSettingsBuilder::new()
                .db_path("./my_db")
                .mem_table_max_size(20000)
                .index_cache(true)
                .value_cache(true)
                .compressor(CompressionConfig::best())
                .build()
        );
```
