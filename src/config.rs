use bincode::config;
use bincode::config::Configuration;

pub const BINCODE_CONFIG: Configuration = config::standard();
pub const HEADER_MAGIC_NUMBER: &[u8; 4] = b"SSTB";
pub const FOOTER_MAGIC_NUMBER: &[u8; 4] = b"FTTB";
pub const CURRENT_VERSION: u32 = 2;
pub const HEADER_SIZE: usize = 16;
pub const FOOTER_SIZE: usize = 20;
pub const DEFAULT_DB_PATH: &str = "./db";
pub const DEFAULT_MEM_TABLE_SIZE: u32 = 10000;
pub const DEFAULT_WAL_MAX_SIZE: u64 = 10 * 1024 * 1024;
pub const WAL_LOG_NAME: &str = "wal.log";
pub const WAL_TEMP_LOG_NAME: &str = "wal_temp.log";
pub const DEFAULT_BLOOM_FILTER_ERROR_PROBABILITY: f64 = 0.01;
pub const DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY: usize = 100;
pub const DEFAULT_INDEX_CACHE_MEMORY_LIMIT: usize = 100 * 1024 * 1024;
pub const DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY: usize = 200000;
pub const DEFAULT_VALUE_CACHE_MEMORY_LIMIT: usize = 200 * 1024 * 1024;
