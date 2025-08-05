use thiserror::Error;

pub type TreeResult<T> = Result<T, TreeError>;

#[derive(Error, Debug)]
pub enum TreeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("IO error: {message}")]
    IoExtended { message: String },

    #[error("Serialization error: {message}")]
    Serialization { message: String },

    #[error("Compression error: {message}")]
    Compression { message: String },

    #[error("WAL error: {message}")]
    Wal { message: String },

    #[error("Data corruption detected: {message}")]
    Corruption { message: String },

    #[error("Invalid key: {message}")]
    InvalidKey { message: String },

    #[error("Invalid value: {message}")]
    InvalidValue { message: String },

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Cache error: {message}")]
    Cache { message: String },

    #[error("Bloom filter error: {message}")]
    BloomFilter { message: String },

    #[error("Internal error: {message}")]
    Internal { message: String },
    
    #[error("Transaction error: {message}")]   
    Transaction { message: String },

    #[error("SystemTime error: {message}")]
    SystemTimeError { message: String },
}

impl TreeError {
    pub fn io<T: std::fmt::Display>(message: T) -> Self {
        Self::IoExtended {
            message: message.to_string(),
        }
    }

    pub fn serialization<T: std::fmt::Display>(message: T) -> Self {
        Self::Serialization {
            message: message.to_string(),
        }
    }

    pub fn compression<T: std::fmt::Display>(message: T) -> Self {
        Self::Compression {
            message: message.to_string(),
        }
    }

    pub fn wal<T: std::fmt::Display>(message: T) -> Self {
        Self::Wal {
            message: message.to_string(),
        }
    }

    pub fn corruption<T: std::fmt::Display>(message: T) -> Self {
        Self::Corruption {
            message: message.to_string(),
        }
    }

    pub fn invalid_key<T: std::fmt::Display>(message: T) -> Self {
        Self::InvalidKey {
            message: message.to_string(),
        }
    }

    pub fn invalid_value<T: std::fmt::Display>(message: T) -> Self {
        Self::InvalidValue {
            message: message.to_string(),
        }
    }

    pub fn configuration<T: std::fmt::Display>(message: T) -> Self {
        Self::Configuration {
            message: message.to_string(),
        }
    }

    pub fn cache<T: std::fmt::Display>(message: T) -> Self {
        Self::Cache {
            message: message.to_string(),
        }
    }

    pub fn bloom_filter<T: std::fmt::Display>(message: T) -> Self {
        Self::BloomFilter {
            message: message.to_string(),
        }
    }

    pub fn internal<T: std::fmt::Display>(message: T) -> Self {
        Self::Internal {
            message: message.to_string(),
        }
    }

    pub fn transaction<T: std::fmt::Display>(message: T) -> Self {
        Self::Transaction {
            message: message.to_string(),
        }
    }

    pub fn system_time_error<T: std::fmt::Display>(message: T) -> Self {
        Self::SystemTimeError {
            message: message.to_string(),
        }
    }
}

impl From<bincode::error::EncodeError> for TreeError {
    fn from(err: bincode::error::EncodeError) -> Self {
        TreeError::serialization(format!("Encode error: {}", err))
    }
}

impl From<bincode::error::DecodeError> for TreeError {
    fn from(err: bincode::error::DecodeError) -> Self {
        TreeError::serialization(format!("Decode error: {}", err))
    }
}

impl From<Box<dyn std::error::Error>> for TreeError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        TreeError::internal(format!("Boxed error: {}", err))
    }
}

impl From<std::time::SystemTimeError> for TreeError {
    fn from(err: std::time::SystemTimeError) -> Self {
        TreeError::system_time_error(format!("SystemTimeError: {}", err))   
    }
}