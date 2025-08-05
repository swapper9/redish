use crate::tree::transaction::{TransactionContext, TransactionStatus, VersionStamp};
use crate::tree::tree_error::{TreeError, TreeResult};
use crate::DataValue;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

/// Manages the lifecycle and state of database transactions.
///
/// The `TransactionManager` is responsible for coordinating all transactional operations
/// within the LSM tree database. It provides isolation between concurrent transactions,
/// manages version control, and ensures ACID properties are maintained.
///
/// # Key Features
/// - **Transaction Isolation**: Each transaction has its own isolated write set
/// - **Version Control**: Tracks global and per-key versions for conflict detection
/// - **Concurrency Control**: Thread-safe operations using Arc<RwLock> and Arc<Mutex>
/// - **Conflict Detection**: Validates transactions before commit to prevent conflicts
///
/// # Thread Safety
/// All fields use thread-safe wrappers (`Arc<RwLock>`, `Arc<Mutex>`) to ensure
/// safe concurrent access from multiple threads.
pub struct TransactionManager {
    /// Active transactions indexed by their unique transaction ID.
    ///
    /// Contains all currently running transactions with their complete context
    /// including read sets, write sets, and validation information.
    pub active_transactions: Arc<RwLock<HashMap<u64, TransactionContext>>>,

    /// Counter for generating unique transaction IDs.
    ///
    /// Atomically incremented for each new transaction to ensure uniqueness
    /// across the lifetime of the transaction manager.
    pub next_transaction_id: Arc<Mutex<u64>>,

    /// Global version counter for the entire database.
    ///
    /// Incremented on each successful commit operation and used for
    /// optimistic concurrency control and conflict detection.
    pub global_version: Arc<Mutex<u64>>,

    /// Version information for each key in the database.
    ///
    /// Maps each key to its current version stamp, which includes both
    /// version number and timestamp. Used for detecting conflicts during
    /// transaction validation.
    pub key_versions: Arc<RwLock<HashMap<Vec<u8>, VersionStamp>>>,
}

impl TransactionManager {
    pub(crate) fn new() -> Self {
        TransactionManager {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            next_transaction_id: Arc::new(Mutex::new(1)),
            global_version: Arc::new(Mutex::new(1)),
            key_versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn begin_transaction(&self) -> TreeResult<u64> {
        let mut next_id = self.next_transaction_id.lock().unwrap();
        let tx_id = *next_id;
        *next_id += 1;

        let tx_context = TransactionContext {
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            validation_set: HashSet::new(),
            status: TransactionStatus::Active,
        };

        let mut active_txs = self.active_transactions.write().unwrap();
        active_txs.insert(tx_id, tx_context);

        Ok(tx_id)
    }

    pub(crate) fn write_transaction(&self, tx_id: u64, key: Vec<u8>, value: DataValue) -> TreeResult<()> {
        let mut active_txs = self.active_transactions.write().unwrap();
        let tx_context = active_txs
            .get_mut(&tx_id)
            .ok_or_else(|| TreeError::transaction("Transaction not found"))?;

        tx_context.write_set.insert(key.clone(), value);
        tx_context.validation_set.insert(key);

        Ok(())
    }

    pub(crate) fn rollback_transaction(&self, tx_id: u64) -> TreeResult<()> {
        let mut active_txs = self.active_transactions.write().unwrap();
        if let Some(tx_context) = active_txs.get_mut(&tx_id) {
            tx_context.status = TransactionStatus::Aborted;
            tx_context.write_set.clear();
        }
        active_txs.remove(&tx_id);
        Ok(())
    }

    pub(crate) fn validate_transaction(&self, tx_id: u64) -> TreeResult<bool> {
        let active_txs = self.active_transactions.read().unwrap();
        let tx_context = active_txs.get(&tx_id)
            .ok_or_else(|| TreeError::transaction("Transaction not found"))?;

        let key_versions = self.key_versions.read().unwrap();

        for (key, read_version) in &tx_context.read_set {
            if let Some(current_version) = key_versions.get(key) {
                if current_version.version > read_version.version {
                    return Ok(false);
                }
                if current_version.timestamp > read_version.timestamp {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    pub(crate) fn apply_transaction_changes(&self, tx_id: u64) -> TreeResult<()> {
        let mut key_versions = self.key_versions.write().unwrap();
        let mut global_version = self.global_version.lock().unwrap();

        let active_txs = self.active_transactions.read().unwrap();
        let tx_context = active_txs.get(&tx_id).unwrap();

        for key in tx_context.write_set.keys() {
            *global_version += 1;
            let new_version_stamp = VersionStamp {
                version: *global_version,
                timestamp: SystemTime::now(),
            };
            key_versions.insert(key.clone(), new_version_stamp);
        }

        Ok(())
    }

    pub(crate) fn finalize_transaction(&self, tx_id: u64) -> TreeResult<()> {
        let mut active_txs = self.active_transactions.write().unwrap();
        if let Some(tx_context) = active_txs.get_mut(&tx_id) {
            tx_context.status = TransactionStatus::Committed;
        }
        active_txs.remove(&tx_id);

        Ok(())
    }
}
