use crate::tree::tree_error::{TreeError, TreeResult};
use crate::{DataValue, Tree};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

/// Represents the current state of a database transaction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

/// A version stamp that tracks the version and timestamp of a data item.
#[derive(Debug, Clone)]
pub struct VersionStamp {
    pub version: u64,
    pub timestamp: SystemTime,
}

/// Complete context and state information for a database transaction.
#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub read_set: HashMap<Vec<u8>, VersionStamp>,
    pub write_set: HashMap<Vec<u8>, DataValue>,
    pub validation_set: HashSet<Vec<u8>>,
    pub status: TransactionStatus,
}

impl Tree {
    /// Begins a new transaction and returns its unique identifier.
    ///
    /// This method creates a new transaction context with its own isolated write set.
    /// The transaction will be assigned a unique ID that can be used for subsequent
    /// transactional operations.
    ///
    /// # Returns
    /// - `Ok(u64)` - The unique transaction ID
    /// - `Err(TreeError)` - If the transaction cannot be created
    pub fn begin_transaction(&mut self) -> TreeResult<u64> {
        let tx_manager = self.tx_manager.lock().unwrap();
        tx_manager.begin_transaction()
    }

    /// Retrieves a value from the tree within the context of a transaction.
    ///
    /// This method first checks the transaction's local write set for any uncommitted
    /// changes. If no local changes are found, it falls back to reading from the main
    /// tree storage. The read operation is recorded for transaction validation purposes.
    ///
    /// # Arguments
    /// - `tx_id` - The transaction ID
    /// - `key` - The key to retrieve
    ///
    /// # Returns
    /// - `Ok(Some(Vec<u8>))` - The value if found and not expired
    /// - `Ok(None)` - If the key doesn't exist or the value has expired
    /// - `Err(TreeError)` - If the transaction is invalid or a read error occurs
    pub fn get_tx(&mut self, tx_id: u64, key: &[u8]) -> TreeResult<Option<Vec<u8>>> {
        let local_value = {
            let tx_manager = self.tx_manager.lock().unwrap();
            let active_txs = tx_manager.active_transactions.read().unwrap();

            if let Some(tx_context) = active_txs.get(&tx_id) {
                tx_context.write_set.get(key).cloned()
            } else {
                return Err(TreeError::transaction("Transaction not found"));
            }
        };

        if let Some(value) = local_value {
            if value.is_expired() {
                return Ok(None);
            }
            return Ok(Some(value.data));
        }

        let result = self.get(key)?;

        {
            let tx_manager = self.tx_manager.lock().unwrap();
            let mut active_txs = tx_manager.active_transactions.write().unwrap();

            if let Some(tx_context) = active_txs.get_mut(&tx_id) {
                tx_context.validation_set.insert(key.to_vec());

                let key_versions = tx_manager.key_versions.read().unwrap();
                if let Some(version_stamp) = key_versions.get(key) {
                    tx_context
                        .read_set
                        .insert(key.to_vec(), version_stamp.clone());
                } else if result.is_some() {
                    use crate::tree::transaction::VersionStamp;
                    use std::time::SystemTime;

                    let default_version = VersionStamp {
                        version: 0,
                        timestamp: SystemTime::UNIX_EPOCH,
                    };
                    tx_context.read_set.insert(key.to_vec(), default_version);
                }
            }
        }

        Ok(result)
    }

    /// Stores a key-value pair within the context of a transaction.
    ///
    /// This method adds the key-value pair to the transaction's local write set
    /// without immediately persisting it to the main tree storage. The changes
    /// will only become visible to other transactions after a successful commit.
    ///
    /// # Arguments
    /// - `tx_id` - The transaction ID
    /// - `key` - The key to store
    /// - `value` - The value to associate with the key
    /// - `ttl` - Optional time-to-live duration for the key-value pair
    ///
    /// # Returns
    /// - `Ok(())` - If the operation succeeds
    /// - `Err(TreeError)` - If the transaction is invalid or a write error occurs
    pub fn put_tx(
        &mut self,
        tx_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> TreeResult<()> {
        let data_value = DataValue::new(value, ttl);
        let tx_manager = self.tx_manager.lock().unwrap();
        tx_manager.write_transaction(tx_id, key, data_value)
    }

    /// Commits a transaction, making all its changes permanent and visible to other transactions.
    ///
    /// This method applies all changes from the transaction's write set to the main tree storage.
    /// It handles TTL expiration during commit and ensures that expired values are not persisted.
    /// The transaction is marked as committed and then removed from the active transactions list.
    ///
    /// # Arguments
    /// - `tx_id` - The transaction ID to commit
    ///
    /// # Returns
    /// - `Ok(())` - If the transaction is successfully committed
    /// - `Err(TreeError)` - If the transaction is not found or commit fails
    pub fn commit_transaction(&mut self, tx_id: u64) -> TreeResult<()> {
        let write_set = {
            let tx_manager = self.tx_manager.lock().unwrap();
            let validation_result = tx_manager.validate_transaction(tx_id)?;
            if !validation_result {
                tx_manager.rollback_transaction(tx_id)?;
                return Err(TreeError::transaction("Transaction validation failed - conflicts detected"));
            }

            let active_txs = tx_manager.active_transactions.read().unwrap();
            if let Some(tx_context) = active_txs.get(&tx_id) {
                tx_context.write_set.clone()
            } else {
                return Err(TreeError::transaction("Transaction not found"));
            }
        };

        for (key, value) in write_set {
            if value.is_expired() {
                continue;
            }

            match value.expires_at {
                None => {
                    self.put(key, value.data)?;
                }
                Some(expiry) => match expiry.duration_since(SystemTime::now()) {
                    Ok(remaining_ttl) => {
                        self.put_to_tree(key, value.data, Some(remaining_ttl))?;
                    }
                    Err(_) => {
                        continue;
                    }
                },
            }
        }

        {
            let tx_manager = self.tx_manager.lock().unwrap();
            tx_manager.apply_transaction_changes(tx_id)?;
            tx_manager.finalize_transaction(tx_id)?;
        }

        Ok(())
    }

    /// Rolls back a transaction, discarding all its changes and making them invisible.
    ///
    /// This method cancels the transaction without applying any of its changes to the
    /// main tree storage. All data in the transaction's write set is discarded, and
    /// the transaction is removed from the active transactions list.
    ///
    /// # Arguments
    /// - `tx_id` - The transaction ID to rollback
    ///
    /// # Returns
    /// - `Ok(())` - If the transaction is successfully rolled back
    /// - `Err(TreeError)` - If there's an error during rollback
    pub fn rollback_transaction(&mut self, tx_id: u64) -> TreeResult<()> {
        let tx_manager = self.tx_manager.lock().unwrap();
        tx_manager.rollback_transaction(tx_id)
    }
}
