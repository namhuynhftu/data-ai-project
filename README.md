# Financial Analytics Data Pipeline

This project implements a data pipeline for processing financial transactions and user data. The pipeline includes data collection, validation, transformation, and analysis components.

## Project Structure

```
fa-dae2-capstone-namhuynh/
├── config/               # Configuration files
├── data/                # Data directory
│   └── external/        # External data sources
├── docs/                # Documentation
│   └── research/        # Research and analysis
├── scripts/             # Utility scripts
│   └── data_collection/ # Data collection scripts
└── tests/               # Test files
```

## Data Sources

The project uses both simulated and external data sources:

### User Data
- Format: JSON and CSV
- Location: `data/external/fake_user_data.{json,csv}`
- Schema:
  - id (UUID): Unique identifier
  - name (string): User's full name
  - email (string): User's email address
  - address (string): User's physical address
  - created_at (ISO datetime): Account creation timestamp

### Transaction Data
- Format: JSON and CSV
- Location: `data/external/fake_transaction_data.{json,csv}`
- Schema:
  - transaction_id (UUID): Unique transaction identifier
  - user_id (UUID): Associated user identifier
  - amount (float): Transaction amount
  - currency (string): Currency code
  - timestamp (ISO datetime): Transaction timestamp

## Configuration

The project requires several environment variables to be set. Copy `env_example.txt` to `.env` and configure the following:

### API Configuration
- `API_KEY`: Your API key for external services
- `API_URL`: Base URL for API endpoints (default: https://jsonplaceholder.typicode.com)
- `MAX_RECORDS`: Maximum number of records to process (default: 100)
- `BATCH_SIZE`: Number of records per batch (default: 10)
- `MAX_RETRIES`: Maximum retry attempts for failed requests (default: 3)

### Logging Configuration
- `LOG_LEVEL`: Logging verbosity level (default: INFO)
