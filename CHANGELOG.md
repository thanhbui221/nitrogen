# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Enhanced FilterTransformer to support complex SQL conditions with AND/OR operators and parentheses grouping
- Improved FilterTransformer documentation with clear examples of complex conditions
- Simplified FilterTransformer implementation and error handling
- Enhanced JoinTransformer to support multiple sequential joins in a single transformation
- Updated join configuration structure in YAML to use a list of joins
- Each join can now specify its own join type, conditions, and column selection
- Improved column handling after joins with explicit column selection

## [0.2.0] - 2025-05-30

### Added
- Enhanced Parquet support:
  - New ParquetExtractor with partition filtering and column selection
  - Improved ParquetLoader with compression and partition overwrite modes
- Organized utility functions into modules:
  - spark_utils.py: Session management and DataFrame operations
  - validation_utils.py: Data validation and quality checks
  - date_utils.py: Date/time handling functions
  - string_utils.py: String manipulation and cleaning
  - logging_utils.py: Logging configuration and job logging
- Standardized data directory structure for local testing:
  - input/: Source data files
  - output/processed/: Final processed data
  - temp/: Temporary processing files
  - archive/: Archived input files

## [0.1.0] - 2025-05-29

### Added
- Initial ETL framework implementation
- Factory pattern for dynamic component creation
- Configuration-driven job execution
- Base classes for extractors, transformers, and loaders
- CSV file extractor implementation
- Database extractor with support for:
  - PostgreSQL
  - MySQL
  - Microsoft SQL Server
  - Oracle
- Common transformers:
  - Column rename
  - Filter
  - Add column
- Parquet and database loaders
- Docker support with:
  - Pre-installed JDBC drivers
  - Environment variable configuration
  - Volume mounting for jobs and data
- Comprehensive documentation
- Unit tests for transformers

### Technical Details
- PySpark 3.5.0 compatibility
- Python 3.x support
- YAML-based configuration
- Environment variable support for credentials
- Docker image based on openjdk:11-slim

[Unreleased]: https://github.com/username/project/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/username/project/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/username/project/releases/tag/v0.1.0 