use std::collections::{BTreeMap, BinaryHeap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use crc32fast::Hasher;
use log::{debug, error};
use crate::{Tree, DataValue, util};
use crate::config::{CURRENT_VERSION, FOOTER_MAGIC_NUMBER, FOOTER_SIZE, HEADER_MAGIC_NUMBER};

impl Tree {
    pub(crate) fn read_key_from_sstable(&mut self, path: &PathBuf, key: &[u8]) -> Option<DataValue> {
        if self.settings.enable_value_cache {
            if let Some(cached_value) = self.value_cache.get(path, key) {
                if !cached_value.is_expired() {
                    return Some(cached_value);
                } else {
                    self.value_cache.remove(path, key);
                }
            }
        }

        if self.settings.enable_index_cache {
            if let Some(cached_index) = self.index_cache.get(path) {
                if let Some(&offset) = cached_index.get(key) {
                    let file = File::open(path).ok()?;
                    let mut reader = BufReader::new(file);
                    match self.read_data_entry(&mut reader, offset) {
                        Ok(data_value) => {
                            if self.settings.enable_value_cache {
                                self.value_cache.put(path.clone(), key.to_vec(), data_value.clone());
                            }
                            return Some(data_value)
                        }
                        Err(e) => {
                            error!("Error reading data entry from SSTable {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        if self.validate_header(&mut reader).is_err() {
            return None;
        }

        let index_offset = self.read_footer(&mut reader).ok()?;
        let data_offset = self.find_key_in_index(&mut reader, index_offset, key)?;

        if self.settings.enable_index_cache {
            if let Ok(index) = self.read_index(&mut reader, index_offset) {
                self.index_cache.put(path.clone(), index);
            }
        }

        match self.read_data_entry(&mut reader, data_offset) {
            Ok(data_value) => {
                if self.settings.enable_value_cache {
                    self.value_cache.put(path.clone(), key.to_vec(), data_value.clone());
                }
                Some(data_value)
            }
            Err(e) => {
                log::error!("Error reading data entry from SSTable {:?} with offset {:?}: {}", path, data_offset, e);
                None
            }
        }
    }

    fn validate_header(&self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != HEADER_MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incorrect header magic number",
            ));
        }

        let mut version = [0u8; 4];
        reader.read_exact(&mut version)?;
        let version = u32::from_le_bytes(version);

        if version != CURRENT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported version: {}", version),
            ));
        }

        // Skipping other header bytes, as they are reserved for now
        let mut reserved = [0u8; 8];
        reader.read_exact(&mut reserved)?;

        Ok(())
    }

    fn read_footer(&self, reader: &mut BufReader<File>) -> std::io::Result<u64> {
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut index_offset_bytes = [0u8; 8];
        reader.read_exact(&mut index_offset_bytes)?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != FOOTER_MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incorrect footer magic number",
            ));
        }

        Ok(index_offset)
    }

    fn read_index(
        &self,
        reader: &mut BufReader<File>,
        offset: u64,
    ) -> std::io::Result<BTreeMap<Vec<u8>, u64>> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut count_bytes = [0u8; 4];
        reader.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes);

        let mut index = BTreeMap::new();

        for _ in 0..count {
            let mut key_len_bytes = [0u8; 4];
            reader.read_exact(&mut key_len_bytes)?;
            let key_len = u32::from_le_bytes(key_len_bytes) as usize;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            let mut offset_bytes = [0u8; 8];
            reader.read_exact(&mut offset_bytes)?;
            let data_offset = u64::from_le_bytes(offset_bytes);

            index.insert(key, data_offset);
        }

        Ok(index)
    }

    pub(crate) fn load_sstable(&self, path: &PathBuf) -> BTreeMap<Vec<u8>, DataValue> {
        let mut table = BTreeMap::new();

        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);

                if let Err(e) = self.validate_header(&mut reader) {
                    log::error!("Wrong header SSTable {:?}: {}", path, e);
                    return table;
                }

                let index_offset = match self.read_footer(&mut reader) {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        log::error!("Error reading footer SSTable {:?}: {}", path, e);
                        return table;
                    }
                };

                let index = match self.read_index(&mut reader, index_offset) {
                    Ok(idx) => idx,
                    Err(e) => {
                        log::error!("Error reading index SSTable {:?}: {}", path, e);
                        return table;
                    }
                };

                for (key, offset) in index {
                    if let Ok(value) = self.read_data_entry(&mut reader, offset) {
                        table.insert(key, value);
                    }
                }
            }
            Err(e) => {
                log::error!("Error opening SSTable {:?}: {}", path, e);
            }
        }

        table
    }

    pub(crate) fn write_sstable(&mut self, table: &BTreeMap<Vec<u8>, DataValue>) -> PathBuf {
        let new_sstable_number = match util::find_last_sstable_number(&self.settings.db_path) {
            None => 0,
            Some(number) => number + 1,
        };
        let table_path = self
            .settings
            .db_path
            .join(format!("sstable_{}.sst", new_sstable_number));
        let file = File::create(&table_path).unwrap();
        let mut writer = BufWriter::new(file);

        self.write_header(&mut writer).unwrap();

        let mut index = BTreeMap::new();

        for (key, value) in table {
            let offset = writer.stream_position().unwrap();
            self.write_data_entry(&mut writer, key, value).unwrap();

            index.insert(key.clone(), offset);
        }

        let index_offset = writer.stream_position().unwrap();
        self.write_index(&mut writer, &index).unwrap();

        self.write_footer(&mut writer, index_offset).unwrap();

        writer.flush().unwrap();
        if self.settings.enable_index_cache {
            self.index_cache.put(table_path.clone(), index);
        }
        table_path
    }

    fn write_header(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_all(HEADER_MAGIC_NUMBER)?;
        writer.write_all(&CURRENT_VERSION.to_le_bytes())?;
        writer.write_all(&[0u8; 8])?; // compression, checksum_type, reserved
        Ok(())
    }

    fn write_data_entry(
        &self,
        writer: &mut BufWriter<File>,
        key: &[u8],
        value: &DataValue,
    ) -> std::io::Result<()> {
        let value_bytes = bincode::encode_to_vec(value, self.settings.bincode_config).unwrap();

        writer.write_all(&(key.len() as u32).to_le_bytes())?;
        writer.write_all(key)?;

        writer.write_all(&(value_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&value_bytes)?;

        let mut hasher = Hasher::new();
        hasher.update(key);
        hasher.update(&value_bytes);
        let checksum = hasher.finalize();
        writer.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    fn write_index(
        &self,
        writer: &mut BufWriter<File>,
        index: &BTreeMap<Vec<u8>, u64>,
    ) -> std::io::Result<()> {
        writer.write_all(&(index.len() as u32).to_le_bytes())?;

        for (index_key, offset) in index {
            writer.write_all(&(index_key.len() as u32).to_le_bytes())?;
            writer.write_all(index_key)?;
            writer.write_all(&offset.to_le_bytes())?;
        }
        Ok(())
    }

    fn write_footer(&self, writer: &mut BufWriter<File>, index_offset: u64) -> std::io::Result<()> {
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(FOOTER_MAGIC_NUMBER)?;
        Ok(())
    }

    pub(crate) fn merge_sstables(&mut self) {
        let tables_to_merge_count = std::cmp::min(self.ss_tables.len(), 3);
        if tables_to_merge_count < 2 {
            return;
        }

        let tables_to_merge: Vec<PathBuf> =
            self.ss_tables.drain(0..tables_to_merge_count).collect();

        let mut table_data: Vec<BTreeMap<Vec<u8>, DataValue>> =
            Vec::with_capacity(tables_to_merge.len());
        for table_path in &tables_to_merge {
            table_data.push(self.load_sstable(table_path));
        }

        let mut iterators: Vec<_> = table_data.iter().map(|table| table.iter()).collect();

        let mut min_heap = BinaryHeap::new();

        for (idx, iterator) in iterators.iter_mut().enumerate() {
            if let Some((key, value)) = iterator.next() {
                min_heap.push(HeapEntry {
                    key: key.clone(),
                    value: value.clone(),
                    table_index: idx,
                });
            }
        }

        let mut merged_data = BTreeMap::new();
        let mut last_key: Option<Vec<u8>> = None;

        while let Some(entry) = min_heap.pop() {
            if entry.value.is_empty() || entry.value.is_tombstone {
                continue;
            }

            let HeapEntry {
                key,
                value,
                table_index,
            } = entry;

            if let Some(ref last) = last_key {
                if *last == key {
                    if let Some((next_key, next_value)) = iterators[table_index].next() {
                        min_heap.push(HeapEntry {
                            key: next_key.clone(),
                            value: next_value.clone(),
                            table_index,
                        });
                    }
                    continue;
                }
            }

            last_key = Some(key.clone());
            merged_data.insert(key, value);
            if let Some((next_key, next_value)) = iterators[table_index].next() {
                min_heap.push(HeapEntry {
                    key: next_key.clone(),
                    value: next_value.clone(),
                    table_index,
                });
            }
        }

        if self.settings.enable_index_cache {
            for path in &tables_to_merge {
                self.index_cache.remove(path);
                self.index_cache.lru_queue.retain(|p| !p.eq(path));
                self.index_cache.remove(path);
            }
        }
        if self.settings.enable_value_cache {
            for path in &tables_to_merge {
                self.value_cache.invalidate_sstable(path);
            }
        }

        let new_table_path = self.write_sstable(&merged_data);
        self.ss_tables.push(new_table_path.clone());

        for path in tables_to_merge {
            if let Err(e) = std::fs::remove_file(&path) {
                log::error!("Error deleting old SSTable {:?}: {}", path, e);
            }
            self.ss_tables.retain(|p| p != &path);
        }
    }

    fn find_key_in_index(
        &self,
        reader: &mut BufReader<File>,
        index_offset: u64,
        key: &[u8],
    ) -> Option<u64> {
        reader.seek(SeekFrom::Start(index_offset)).ok()?;

        let mut index_num_entries_count_bytes = [0u8; 4];
        reader.read_exact(&mut index_num_entries_count_bytes).ok()?;
        let index_num_entries = u32::from_le_bytes(index_num_entries_count_bytes);

        let mut entries = Vec::with_capacity(index_num_entries as usize);

        for index_entry in 0..index_num_entries {
            let mut index_key_len_bytes = [0u8; 4];
            if reader.read_exact(&mut index_key_len_bytes).is_err() {
                error!("Error reading key len for entry {}", index_entry);
                return None;
            }
            let index_key_len = u32::from_le_bytes(index_key_len_bytes) as usize;

            let mut index_key = vec![0u8; index_key_len];
            if reader.read_exact(&mut index_key).is_err() {
                error!("Error reading key for entry {}", index_entry);
                return None;
            }

            let mut data_entry_offset_bytes = [0u8; 8];
            if reader.read_exact(&mut data_entry_offset_bytes).is_err() {
                error!("Error reading offset for entry {}", index_entry);
                return None;
            }
            let data_entry_offset = u64::from_le_bytes(data_entry_offset_bytes);

            entries.push((index_key, data_entry_offset));
        }

        let mut left = 0;
        let mut right = entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let (index_key, offset) = &entries[mid];

            match index_key.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => {
                    debug!("Key found in index: {:?}", String::from_utf8_lossy(key));
                    return Some(*offset);
                }
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => right = mid,
            }
        }

        debug!("Key not found in index: {:?}", String::from_utf8_lossy(key));
        None
    }

    fn read_data_entry(
        &self,
        reader: &mut BufReader<File>,
        offset: u64,
    ) -> std::io::Result<DataValue> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut key_len_bytes = [0u8; 4];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;

        reader.seek(SeekFrom::Current(key_len as i64))?;

        let mut value_len_bytes = [0u8; 4];
        reader.read_exact(&mut value_len_bytes)?;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;

        let mut value_bytes = vec![0u8; value_len];
        reader.read_exact(&mut value_bytes)?;

        match bincode::decode_from_slice(&value_bytes, self.settings.bincode_config) {
            Ok((decoded, _)) => Ok(decoded),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Deserialization error: {}", e),
            )),
        }
    }

    pub(crate) fn validate_sstable(&self, path: &PathBuf) -> bool {
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                if self.validate_header(&mut reader).is_err() {
                    log::error!("Error validating header SSTable : {:?}", path);
                    return false;
                }
                if self.read_footer(&mut reader).is_err() {
                    log::error!("Error validating footer SSTable {:?}", path);
                    return false;
                }
                true
            }
            Err(e) => {
                log::error!("Error opening SSTable {:?}: {}", path, e);
                false
            }
        }
    }
}

#[derive(Debug, Eq)]
struct HeapEntry {
    key: Vec<u8>,
    value: DataValue,
    table_index: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reverse order
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
