# Billcom Data Migration Project

A Python ETL project that migrates customer records from a MySQL source table to a cleaned destination table.

The project includes two execution modes:
- `single processing/`: single-process ETL flow.
- `Multiprocesses/`: chunked multiprocessing ETL flow.

## What It Does

1. Extracts rows from `custemer_source`.
2. Validates and corrects email, phone number, and date of birth fields.
3. Loads cleaned data into `main_table` with generated UUIDs.
4. Exports invalid/corrected rows to CSV reports.

## Project Structure

- `Multiprocesses/main.py`: multiprocessing ETL orchestrator.
- `Multiprocesses/database_creator.py`: seed/generate source data for testing.
- `Multiprocesses/modules/`: extract/transform/load, validation, logging helpers.
- `single processing/main.py`: single-process ETL entry point.
- `single processing/moduls/`: database and validation helpers for single mode.

## Requirements

- Python 3.10+
- MySQL server
- Python packages:
  - `pandas`
  - `PyYAML`
  - `mysql-connector-python`

Install dependencies:

```bash
pip install pandas pyyaml mysql-connector-python
```

## Configuration

Both modes read settings from a `config.yaml` file in their folder.

Key settings include:
- MySQL host/user/password/database
- input table name
- chunk/process settings
- output directory paths

## Usage

### 1) Generate sample data (optional)

```bash
python "Multiprocesses/database_creator.py" --config "Multiprocesses/config.yaml" --repeat 1000
```

### 2) Run multiprocessing ETL

```bash
python "Multiprocesses/main.py" --config "Multiprocesses/config.yaml"
```

### 3) Run single-process ETL

```bash
python "single processing/main.py" --config "single processing/config.yaml"
```

## Outputs

- Cleaned records inserted into MySQL `main_table`.
- CSV outputs created under mode-specific `directory/` folders.
- Multiprocessing mode also creates `filtered_output.csv` from transformed chunks.

## Notes

- Current sample configs contain plaintext DB credentials; use local environment-specific configs and avoid committing secrets.
- Some folder/file names are intentionally kept as-is to match existing code imports.

## License

See `LICENSE`.
