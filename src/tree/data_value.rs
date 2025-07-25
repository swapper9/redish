use bincode::{Decode, Encode};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Encode, Decode, Eq, PartialEq)]
pub struct DataValue {
    pub data: Vec<u8>,
    pub expires_at: Option<SystemTime>,
    pub created_at: SystemTime,
    pub is_tombstone: bool,
}

impl DataValue {
    /// Creates a new DataValue with the specified data and optional TTL.
    ///
    /// # Arguments
    /// * `data` - The raw data to store as bytes
    /// * `ttl` - Optional time-to-live duration. If None, the value never expires
    ///
    /// # Returns
    /// A new DataValue instance with creation timestamp and calculated expiration time
    pub fn new(data: Vec<u8>, ttl: Option<Duration>) -> Self {
        let created_at = SystemTime::now();
        let expires_at = ttl.map(|duration| created_at + duration);

        Self {
            data,
            expires_at,
            created_at,
            is_tombstone: false,
        }
    }

    /// Checks if the stored data is empty.
    ///
    /// # Returns
    /// `true` if the data vector is empty, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Checks if the value has expired based on its TTL.
    ///
    /// # Returns
    /// `true` if the value has expired, `false` if it's still valid or has no expiration
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            SystemTime::now() > expiry
        } else {
            false
        }
    }

    /// Returns a reference to the stored data.
    ///
    /// # Returns
    /// A byte slice reference to the stored data
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    /// Creates a tombstone marker for deletion.
    ///
    /// A tombstone is a special marker that indicates a key has been deleted.
    /// It's used internally for proper deletion semantics in LSM trees.
    ///
    /// # Returns
    /// A new DataValue instance marked as a tombstone
    pub fn tombstone() -> Self {
        Self {
            data: Vec::new(),
            expires_at: None,
            created_at: SystemTime::now(),
            is_tombstone: true,
        }
    }

    /// Checks if this value is a tombstone (deletion marker).
    ///
    /// # Returns
    /// `true` if this is a tombstone, `false` otherwise
    pub fn is_tombstone(&self) -> bool {
        self.is_tombstone
    }
}