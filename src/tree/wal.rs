use crate::tree::tree_error::{TreeError, TreeResult};
use crate::tree::wal_reader::WalReader;
use crate::tree::wal_writer::WalWriter;
use crate::{DataValue, Tree};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::mpsc;

pub(crate) enum WalOperation {
    Checkpoint = 1,
    Put = 2,
    Delete = 3,
}

impl WalOperation {
    pub(crate) fn to_u8(&self) -> u8 {
        match self {
            WalOperation::Checkpoint => 1,
            WalOperation::Put => 2,
            WalOperation::Delete => 3,
        }
    }
}

impl Tree {
    pub(crate) fn init_wal(&mut self) -> TreeResult<()> {
        if !&self.settings.db_path.exists() {
            std::fs::create_dir_all(&self.settings.db_path)
                .map_err(|e| TreeError::wal(format!("Failed to create DB directory: {}", e)))?;
        }

        let (wal_segment_paths, wal_segments) = self.find_wal_segments()?;
        self.wal_segments = wal_segments;

        if wal_segment_paths.is_empty() {
            let segment_num = self.get_next_wal_segment_number();
            self.add_wal_segment(segment_num);
            let wal_path = &self.settings.db_path.join(format!("wal_{:04}.log", segment_num));
            let writer = WalWriter::open(&wal_path)
                .map_err(|e| TreeError::wal(format!("Failed to initialize WAL: {}", e)))?;
            self.wal_writer = Some(writer);

            Ok(())
        } else {
            let segment_num = self.get_last_wal_segment_number();
            let wal_path = &self.settings.db_path.join(format!("wal_{:04}.log", segment_num));
            let mut reader = WalReader::open(wal_path)?;

            if reader.has_checkpoint_at_end()? {
                let next_segment_num = self.get_next_wal_segment_number();
                self.add_wal_segment(next_segment_num);
                let writer = WalWriter::open(&wal_path)
                    .map_err(|e| TreeError::wal(format!("Failed to initialize WAL: {}", e)))?;
                self.wal_writer = Some(writer);
            } else {
                let wal_path = &self.settings.db_path.join(format!("wal_{:04}.log", segment_num));
                let writer = WalWriter::open(&wal_path)
                    .map_err(|e| TreeError::wal(format!("Failed to initialize WAL: {}", e)))?;
                self.wal_writer = Some(writer);
            }

            Ok(())
        }
    }

    pub(crate) fn write_to_wal(
        &mut self,
        op: WalOperation,
        key: &[u8],
        data_value_opt: Option<&DataValue>,
    ) -> TreeResult<()> {
        let should_checkpoint = self.should_checkpoint_wal();

        if let Some(ref mut wal_writer) = self.wal_writer {
            wal_writer.write_entry(op, &key, data_value_opt)
                .map_err(|e| TreeError::wal(format!("Failed to write to WAL: {}", e)))?;

            if should_checkpoint {
                wal_writer.write_checkpoint()
                    .map_err(|e| TreeError::wal(format!("Failed to write checkpoint: {}", e)))?;
            }
        }

        Ok(())
    }

    fn find_wal_segments(&self) -> TreeResult<(Vec<PathBuf>, Vec<u16>)> {
        let entries = std::fs::read_dir(&self.settings.db_path)
            .map_err(|e| TreeError::wal(format!("Failed to read DB directory: {}", e)))?;
        let mut wal_files = Vec::new();
        let mut wal_files_nums = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| TreeError::wal(format!("Failed to read directory entry: {}", e)))?;
            let path = entry.path();

            if path.is_file() {
                if let Some(filename) = path.clone().file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("wal_") && filename.ends_with(".log") {
                        wal_files.push(path);
                        let wal_file_num = filename.strip_prefix("wal_").unwrap()
                            .strip_suffix(".log").unwrap()
                            .parse::<u16>()
                            .unwrap_or(0);
                        wal_files_nums.push(wal_file_num);
                    }
                }
            }
        }

        if wal_files.is_empty() {
            return Ok((wal_files, wal_files_nums));
        }

        wal_files.sort_by_cached_key(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| {
                    name.strip_prefix("wal_")?
                        .strip_suffix(".log")?
                        .parse::<u16>()
                        .ok()
                })
                .unwrap_or(0)
        });
        wal_files_nums.sort();

        Ok((wal_files, wal_files_nums))
    }

    pub(crate) fn create_new_wal_segment(&mut self, segment_num: u16) -> TreeResult<()> {
        let wal_path = self.settings.db_path.join(format!("wal_{:04}.log", segment_num));

        self.wal_writer = None;

        let new_writer = WalWriter::open(&wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to create new WAL segment: {}", e)))?;

        self.wal_writer = Some(new_writer);
        self.add_wal_segment(segment_num);

        debug!("Created new WAL segment: wal_{:04}.log", segment_num);
        Ok(())
    }

    pub(crate) fn wal_background_cleanup_worker(receiver: mpsc::Receiver<u16>, db_path: PathBuf) {
        for segment_num in receiver {
            let wal_file_path = db_path.join(format!("wal_{:04}.log", segment_num));
            if wal_file_path.exists() {
                if let Err(e) = std::fs::remove_file(&wal_file_path) {
                    error!("Failed to remove WAL segment {:04}: {}", segment_num, e);
                } else {
                    debug!("Removed old WAL segment: wal_{:04}.log", segment_num);
                }
            }
        }
    }

    pub(crate) fn get_next_wal_segment_number(&self) -> u16 {
        self.wal_segments.iter().max().unwrap_or(&0) + 1
    }

    pub(crate) fn get_last_wal_segment_number(&self) -> u16 {
        self.wal_segments.iter().max().copied().unwrap_or(0)
    }

    fn add_wal_segment(&mut self, segment_num: u16) {
        self.wal_segments.push(segment_num);
    }

    pub(crate) fn schedule_wal_segment_cleanup(&self, segments_to_remove: &Vec<u16>) {
        if let Some(ref sender) = self.cleanup_sender {
            for segment_num in segments_to_remove {
                if let Err(e) = sender.send(*segment_num) {
                    error!("Failed to schedule WAL segment {} for cleanup: {}", segment_num, e);
                }
            }
        }
    }

    pub(crate) fn remove_obsolete_wal_segments(&self) {
        let segments = &*self.wal_segments;
        const SEGMENTS_TO_KEEP: usize = 3;

        if segments.len() > SEGMENTS_TO_KEEP {
            let segments_to_remove_count = segments.len() - SEGMENTS_TO_KEEP;
            let obsolete_wal_segments: Vec<u16> = segments.iter()
                .take(segments_to_remove_count)
                .copied()
                .collect();

            if !obsolete_wal_segments.is_empty() {
                self.schedule_wal_segment_cleanup(&obsolete_wal_segments);
            }
        }
    }

    fn should_checkpoint_wal(&self) -> bool {
        let last_wal_segment_number = self.get_last_wal_segment_number();
        let wal_path = self.settings.db_path
            .join(format!("wal_{:04}.log", last_wal_segment_number));

        if let Ok(metadata) = std::fs::metadata(wal_path) {
            metadata.len() > self.settings.wal_max_size
        } else {
            false
        }
    }

    pub(crate) fn recover_from_wal(&mut self) -> TreeResult<()> {
        let (wal_segment_paths, wal_segments) = self.find_wal_segments()?;
        self.wal_segments = wal_segments;

        let mut all_entries = Vec::new();

        for wal_path in &wal_segment_paths {
            let mut reader = WalReader::open(wal_path)
                .map_err(|e| TreeError::wal(format!("Failed to open WAL {:?} for recovery: {}", wal_path, e)))?;

            if reader.has_checkpoint_at_end()? {
                continue;
            }
            let entries = reader.read_entries()
                .map_err(|e| TreeError::wal(format!("Failed to read WAL entries from {:?}: {}", wal_path, e)))?;

            all_entries.extend(entries);
        }

        let mut recovered_count = 0;
        for (op, key, data_value) in all_entries.into_iter() {
            match op {
                WalOperation::Put => {
                    self.mem_table.insert(key, data_value);
                    recovered_count += 1;
                }
                WalOperation::Delete => {
                    self.mem_table.insert(key, DataValue::tombstone());
                    recovered_count += 1;
                }
                WalOperation::Checkpoint => {
                    continue;
                }
            }
        }

        info!("Recovered {} entries from WAL", recovered_count);
        self.init_wal()?;

        Ok(())
    }

    pub(crate) fn check_wal_segments_need_to_be_shifted(&mut self) -> TreeResult<()> {
        let max_segment = self.get_last_wal_segment_number();
        if max_segment > 10 {
            self.rename_wal_segments_from_zero()?;
        }

        Ok(())
    }

    fn rename_wal_segments_from_zero(&mut self) -> TreeResult<()> {
        let (_, segments) = self.find_wal_segments()?;
        let mut new_segments = Vec::new();

        for (new_index, &old_segment_num) in segments.iter().enumerate() {
            let old_path = self.settings.db_path.join(format!("wal_{:04}.log", old_segment_num));
            let new_path = self.settings.db_path.join(format!("wal_{:04}.log", new_index));

            if old_path.exists() && old_segment_num != new_index as u16 {
                std::fs::rename(&old_path, &new_path)
                    .map_err(|e| TreeError::wal(
                        format!("Error renaming WAL segment {} -> {}: {}",
                                old_segment_num, new_index, e)
                    ))?;

                debug!("WAL segment renamed: {} -> {}", old_segment_num, new_index);
            }
            new_segments.push(new_index as u16);
        }
        self.wal_segments = new_segments;

        if let Some(_) = self.wal_writer {
            if let Some(&current_segment) = self.wal_segments.last() {
                self.wal_writer = None;
                let current_wal_path = self.settings.db_path.join(format!("wal_{:04}.log", current_segment));
                let writer = WalWriter::open(&current_wal_path)
                    .map_err(|e| TreeError::wal(format!("Failed to initialize WAL: {}", e)))?;
                self.wal_writer = Some(writer);
            }
        }

        debug!("WAL segments renaming complete. New numbers: {:?}", self.wal_segments);
        Ok(())
    }
}