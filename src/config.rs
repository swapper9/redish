use bincode::config;
use bincode::config::Configuration;

pub const BINCODE_CONFIG: Configuration = config::standard();
pub const HEADER_MAGIC_NUMBER: &[u8; 4] = b"SSTB";
pub const FOOTER_MAGIC_NUMBER: &[u8; 4] = b"FTTB";
pub const CURRENT_VERSION: u32 = 1;
pub const HEADER_SIZE: usize = 16;
pub const FOOTER_SIZE: usize = 12;
pub const DEFAULT_DB_PATH: &str = "./db";
pub const DEFAULT_MEM_TABLE_SIZE: u32 = 10000;
