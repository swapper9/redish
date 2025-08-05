# Changelog

## [0.5.0] - 2025-08-06

### Added
- **Transaction System**: Added support for optimistic transaction management
    - `begin_transaction()` - start a new transaction
    - `commit_transaction(tx_id)` - commit a transaction
    - `rollback_transaction(tx_id)` - rollback a transaction
    - `put_tx()`, `get_tx()`, `delete_tx()` - operations within transaction scope
- New modules:
    - `transaction.rs` - core transaction functionality
    - `transaction_manager.rs` - transaction lifecycle management
- Added `transaction_id` field to `DataValue` structure for tracking transactional changes

### Changed
- **Disabled compression by default**: Changed default setting from LZ4 to no compression
- Updated documentation with transaction usage examples
- Improved index caching settings description in README
- Fixed code example in README (`b"key1".to_string().into_bytes()`)

## [0.4.0] - 2025-08-01

### Added
- **Write-Ahead Logging (WAL)**: Complete WAL implementation for data durability
    - WAL segment management and rotation
    - Automatic recovery from WAL on database startup
    - Background WAL segment cleanup
    - WAL segment renaming and optimization
- Added WAL-specific error handling and result wrappers for public methods
- Added milliseconds to logging output for better debugging

### Changed
- Simplified LRU cache builders, integrated into TreeSettingsBuilder
- Enhanced error handling with proper Result types for all public APIs

## [0.3.0] - 2025-07-22

### Added
- **SSTable Auto-Renaming**: Automatic renaming of SSTable files for better organization
- **CompressionConfig::none()**: Added option for no compression
- **Improved Bloom Filter**: Optimized default error probability to 0.01 for better performance
- **Enhanced Value Cache**: Default cache increased to 200k entries with 200MB memory limit
- Added minimum supported Rust version (MSRV) specification

### Changed
- Bumped all dependencies to latest versions
- Default bloom filter error probability set to optimal 0.01
- Value cache settings optimized for better performance

### Internal Changes
- SSTable versioning updated to v.2

## [0.2.0] - 2025-07-16 to 2025-07-21

### Added
- **Data Compression**: Full compression support with multiple algorithms
    - LZ4, Zstd, and Snappy compression options
    - Configurable compression levels and settings
    - Compression statistics and monitoring
- **LRU Value Caching**: Complete LRU cache implementation for frequently accessed values
- **LRU Index Caching**: Efficient caching system for SSTable indexes
- **Cache Statistics**: Comprehensive cache performance monitoring
- **Modular Architecture**: Refactored tree into multiple specialized modules
- **Builder Pattern**: Added builders and comprehensive documentation

### Changed
- Major performance optimizations for get operations

### Internal Changes
- Tree codebase refactored into multiple specialized modules
- Cache statistics display and monitoring
- Better memory management and optimization

## [0.1.0] - 2025-07-09

### Added
- **Initial Release**: Basic LSM Tree implementation
- **Core Features**:
    - In-memory key-value storage with BTree-based memtable
    - SSTable persistence with binary format
    - TTL (Time-To-Live) support for automatic key expiration
    - Type-friendly API with generic support
    - Basic compression and serialization
- **Storage Engine**:
    - Memory table with configurable size limits
    - SSTable file format with index and data blocks
    - CRC32 checksums for data integrity
    - Binary serialization with bincode
