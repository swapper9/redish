//! Redish - simple in-memory key-value database with TTL support
//!
//! # Example
//! ```
//! use std::time::Duration;
//! use redish::tree::Tree;
//! use bincode::{Decode, Encode};
//!
//! #[derive(Debug, Encode, Decode, Clone)]
//! struct User {
//!     user_id: u64,
//!     username: String,
//! }
//! let user = User {user_id: 3, username: "JohnDoe2020".to_string()};
//!
//! let mut tree = Tree::load_with_path("/path/to/db/with_file_name");
//! tree.put("key1".to_string().into_bytes(), "value".to_string().into_bytes());
//! tree.put_with_ttl("key2".to_string().into_bytes(), "value".to_string().into_bytes(), Some(Duration::from_secs(60)));
//! tree.put_typed::<User>("key3", &user);
//! ```

pub mod tree;
pub mod util;
pub mod config;
mod logger;

pub use crate::tree::{Tree, DataValue, TreeSettings, TreeSettingsBuilder};
pub use bincode::{Decode, Encode};
