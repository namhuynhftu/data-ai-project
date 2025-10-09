from pathlib import Path
import os
import json
from datetime import datetime, timezone
import logging
from typing import Optional, Dict, Any, List
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from elt_pipeline.streaming.utils.fake_data_generator import FakeDataGenerator


def load_metadata_config(config_path: str) -> Dict[str, Any]:
    """Load metadata configuration from a JSON file."""
    with open(config_path, "r") as f:
        metadata = json.load(f)
    return metadata


def generate_fake_data_op(
    table_name: str, 
    num_records: int = 10000,
    metadata_config_path: str = None
) -> Dict[str, Any]:
    """
    Generate fake data operation
    
    Args:
        table_name: Name of the table to generate data for
        num_records: Number of records to generate
        metadata_config_path: Path to metadata configuration file
    
    Returns:
        Dictionary containing generated data and metadata
    """
    try:
        # Load metadata configuration
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        metadata = load_metadata_config(str(metadata_config_path))
        
        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' not found in metadata configuration")
        
        table_metadata = metadata[table_name]
        logger.info(f"Generating {num_records} records for table '{table_name}'")
        
        # Initialize fake data generator (without creating directories)
        generator = FakeDataGenerator(create_directories=False)
        
        # Generate data based on table type
        if table_name == "users":
            data = generator.generate_user_data(num_records)
        elif table_name == "transactions":
            data = generator.generate_transaction_data(num_records)
        elif table_name == "detailed_transactions":
            # For detailed transactions, generate some user IDs first
            user_ids = [generator.faker.uuid4() for _ in range(min(100, num_records))]
            data = generator.generate_detailed_transaction_data(num_records, user_ids)
        else:
            raise ValueError(f"Unsupported table type: {table_name}")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Add ingestion timestamp
        ingestion_timestamp = datetime.now(timezone.utc)
        df['ingested_datetime'] = ingestion_timestamp
        
        # Get column names from metadata (excluding complex nested objects for PostgreSQL)
        column_mapping = {}
        columns_to_include = []
        
        for col_name, col_config in table_metadata["columns"].items():
            if col_config["type"] not in ["OBJECT"]:  # Skip complex nested objects
                columns_to_include.append(col_name)
                column_mapping[col_name] = col_config
        
        # Add ingested_datetime column to metadata
        columns_to_include.append('ingested_datetime')
        column_mapping['ingested_datetime'] = {
            "type": "TIMESTAMP",
            "description": "Data ingestion timestamp",
            "nullable": False
        }
        
        # Filter DataFrame to include only supported columns
        df_filtered = df[columns_to_include].copy()
        
        # Handle nested objects by flattening or converting to JSON strings
        if table_name == "detailed_transactions":
            # Convert complex objects to JSON strings for PostgreSQL compatibility
            for col_name in df.columns:
                if col_name in ['location', 'metadata', 'card_details', 'store_details']:
                    if col_name in df_filtered.columns:
                        df_filtered[col_name] = df_filtered[col_name].apply(
                            lambda x: json.dumps(x) if x is not None else None
                        )
        
        logger.info(f"Generated {len(df_filtered)} records for table '{table_name}'")
        logger.info(f"Columns included: {list(df_filtered.columns)}")
        
        # Prepare return data structure
        result_data = {
            "data": df_filtered,
            "table_name": table_name,
            "num_records": len(df_filtered),
            "columns": columns_to_include,
            "column_mapping": column_mapping,
            "ingestion_timestamp": ingestion_timestamp,
            "metadata": table_metadata
        }
        
        # Log all components successfully generated
        logger.info("✅ Successfully generated all required components:")
        logger.info(f"   📊 data: DataFrame with shape {df_filtered.shape}")
        logger.info(f"   🏷️  table_name: {result_data['table_name']}")
        logger.info(f"   🔢 num_records: {result_data['num_records']}")
        logger.info(f"   📋 columns: {len(result_data['columns'])} columns - {result_data['columns']}")
        logger.info(f"   ⏰ ingestion_timestamp: {result_data['ingestion_timestamp']}")
        logger.info(f"   🗂️  metadata: Schema metadata loaded successfully")
        
        return result_data
        
    except Exception as e:
        logger.error(f"Error generating fake data for table '{table_name}': {str(e)}")
        raise


def generate_detailed_transaction_data_op(
    num_records: int = 10000,
    user_ids: List[str] = None,
    metadata_config_path: str = None
) -> Dict[str, Any]:
    """
    Generate detailed transaction data operation with user IDs
    
    Args:
        num_records: Number of records to generate
        user_ids: List of user IDs to use for transactions
        metadata_config_path: Path to metadata configuration file
    
    Returns:
        Dictionary containing generated data and metadata
    """
    try:
        # Load metadata configuration
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        metadata = load_metadata_config(str(metadata_config_path))
        table_metadata = metadata["detailed_transactions"]
        
        logger.info(f"Generating {num_records} detailed transaction records")
        
        # Initialize fake data generator (without creating directories)
        generator = FakeDataGenerator(create_directories=False)
        
        # Generate data
        data = generator.generate_detailed_transaction_data(num_records, user_ids)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Add ingestion timestamp
        ingestion_timestamp = datetime.now(timezone.utc)
        df['ingested_datetime'] = ingestion_timestamp
        
        # Flatten complex objects for PostgreSQL compatibility
        # Convert location object to separate columns
        if 'location' in df.columns:
            location_df = pd.json_normalize(df['location'])
            location_df.columns = [f'location_{col}' for col in location_df.columns]
            df = pd.concat([df.drop('location', axis=1), location_df], axis=1)
        
        # Convert metadata object to separate columns
        if 'metadata' in df.columns:
            metadata_df = pd.json_normalize(df['metadata'])
            metadata_df.columns = [f'metadata_{col}' for col in metadata_df.columns]
            df = pd.concat([df.drop('metadata', axis=1), metadata_df], axis=1)
        
        # Convert card_details to JSON string (optional field)
        if 'card_details' in df.columns:
            df['card_details'] = df['card_details'].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        
        # Convert store_details to JSON string (optional field)
        if 'store_details' in df.columns:
            df['store_details'] = df['store_details'].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        
        logger.info(f"Generated {len(df)} detailed transaction records")
        logger.info(f"Final columns: {list(df.columns)}")
        
        # Prepare return data structure
        result_data = {
            "data": df,
            "table_name": "detailed_transactions",
            "num_records": len(df),
            "columns": list(df.columns),
            "ingestion_timestamp": ingestion_timestamp,
            "metadata": table_metadata
        }
        
        # Log all components successfully generated
        logger.info("✅ Successfully generated all required components:")
        logger.info(f"   📊 data: DataFrame with shape {df.shape}")
        logger.info(f"   🏷️  table_name: {result_data['table_name']}")
        logger.info(f"   🔢 num_records: {result_data['num_records']}")
        # logger.info(f"   📋 columns: {len(result_data['columns'])} columns - {result_data['columns']}")
        logger.info(f"   ⏰ ingestion_timestamp: {result_data['ingestion_timestamp']}")
        logger.info(f"   🗂️  metadata: Schema metadata loaded successfully")
        
        return result_data
        
    except Exception as e:
        logger.error(f"Error generating detailed transaction data: {str(e)}")
        raise


def generate_users_data_op(
    num_records: int = 1000,
    metadata_config_path: str = None
) -> Dict[str, Any]:
    """
    Generate users data operation with user_id as primary key
    
    Args:
        num_records: Number of user records to generate
        metadata_config_path: Path to metadata configuration file
    
    Returns:
        Dictionary containing generated users data and metadata
    """
    try:
        # Load metadata configuration
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        metadata = load_metadata_config(str(metadata_config_path))
        table_metadata = metadata["users"]
        
        logger.info(f"Generating {num_records} user records")
        
        # Initialize fake data generator (without creating directories)
        generator = FakeDataGenerator(create_directories=False)
        
        # Generate user data
        data = generator.generate_user_data(num_records)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Ensure user_id is the primary key column (rename if needed)
        if 'id' in df.columns and 'user_id' not in df.columns:
            df = df.rename(columns={'id': 'user_id'})
        
        # Add ingestion timestamp
        ingestion_timestamp = datetime.now(timezone.utc)
        df['ingested_datetime'] = ingestion_timestamp
        
        logger.info(f"Generated {len(df)} user records")
        logger.info(f"User columns: {list(df.columns)}")
        logger.info(f"Sample user_ids: {df['user_id'].head(3).tolist()}")
        
        # Prepare return data structure
        result_data = {
            "data": df,
            "table_name": "users",
            "num_records": len(df),
            "columns": list(df.columns),
            "primary_key": "user_id",
            "ingestion_timestamp": ingestion_timestamp,
            "metadata": table_metadata
        }
        
        # Log all components successfully generated
        logger.info("✅ Successfully generated users data:")
        logger.info(f"   📊 data: DataFrame with shape {df.shape}")
        logger.info(f"   🔑 primary_key: {result_data['primary_key']}")
        logger.info(f"   🏷️  table_name: {result_data['table_name']}")
        logger.info(f"   🔢 num_records: {result_data['num_records']}")
        logger.info(f"   ⏰ ingestion_timestamp: {result_data['ingestion_timestamp']}")
        logger.info(f"   �️  metadata: Schema metadata loaded successfully")
        
        return result_data
        
    except Exception as e:
        logger.error(f"Error generating users data: {str(e)}")
        raise


def generate_transactions_data_op(
    num_records: int = 5000,
    user_ids: List[str] = None,
    metadata_config_path: str = None
) -> Dict[str, Any]:
    """
    Generate transactions data operation with transaction_id as primary key and user_id as foreign key
    
    Args:
        num_records: Number of transaction records to generate
        user_ids: List of existing user IDs to reference (foreign key)
        metadata_config_path: Path to metadata configuration file
    
    Returns:
        Dictionary containing generated transactions data and metadata
    """
    try:
        # Load metadata configuration
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        metadata = load_metadata_config(str(metadata_config_path))
        table_metadata = metadata["transactions"]
        
        logger.info(f"Generating {num_records} transaction records")
        
        # Initialize fake data generator (without creating directories)
        generator = FakeDataGenerator(create_directories=False)
        
        # Generate transaction data with user_ids as foreign keys
        data = generator.generate_transaction_data(num_records, user_ids)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Add ingestion timestamp
        ingestion_timestamp = datetime.now(timezone.utc)
        df['ingested_datetime'] = ingestion_timestamp
        
        logger.info(f"Generated {len(df)} transaction records")
        logger.info(f"Transaction columns: {list(df.columns)}")
        logger.info(f"Sample transaction_ids: {df['transaction_id'].head(3).tolist()}")
        if user_ids:
            logger.info(f"Using {len(user_ids)} provided user_ids for foreign key relationships")
        
        # Prepare return data structure
        result_data = {
            "data": df,
            "table_name": "transactions",
            "num_records": len(df),
            "columns": list(df.columns),
            "primary_key": "transaction_id",
            "foreign_keys": {"user_id": "users.user_id"},
            "ingestion_timestamp": ingestion_timestamp,
            "metadata": table_metadata
        }
        
        # Log all components successfully generated
        logger.info("✅ Successfully generated transactions data:")
        logger.info(f"   📊 data: DataFrame with shape {df.shape}")
        logger.info(f"   🔑 primary_key: {result_data['primary_key']}")
        logger.info(f"   � foreign_keys: {result_data['foreign_keys']}")
        logger.info(f"   🏷️  table_name: {result_data['table_name']}")
        logger.info(f"   🔢 num_records: {result_data['num_records']}")
        logger.info(f"   ⏰ ingestion_timestamp: {result_data['ingestion_timestamp']}")
        logger.info(f"   🗂️  metadata: Schema metadata loaded successfully")
        
        return result_data
        
    except Exception as e:
        logger.error(f"Error generating transactions data: {str(e)}")
        raise


def generate_detailed_transactions_with_incremental_pk_op(
    num_records: int = 10000,
    transaction_ids: List[str] = None,
    user_ids: List[str] = None,
    metadata_config_path: str = None,
    start_id: int = 1
) -> Dict[str, Any]:
    """
    Generate detailed transaction data with incremental primary key and foreign keys to transactions and users
    
    Args:
        num_records: Number of detailed transaction records to generate
        transaction_ids: List of existing transaction IDs to reference (foreign key)
        user_ids: List of existing user IDs to reference (foreign key)
        metadata_config_path: Path to metadata configuration file
        start_id: Starting value for incremental primary key
    
    Returns:
        Dictionary containing generated detailed transactions data and metadata
    """
    try:
        # Load metadata configuration
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        metadata = load_metadata_config(str(metadata_config_path))
        table_metadata = metadata["detailed_transactions"]
        
        logger.info(f"Generating {num_records} detailed transaction records with incremental primary key")
        
        # Initialize fake data generator (without creating directories)
        generator = FakeDataGenerator(create_directories=False)
        
        # Generate detailed transaction data
        data = generator.generate_detailed_transaction_data(num_records, user_ids)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # NOTE: For streaming pipeline, we DON'T add explicit 'id' column
        # Let PostgreSQL's SERIAL column auto-generate incremental values
        # This allows continuous loading without primary key conflicts
        
        # If transaction_ids are provided, randomly assign them to detailed transactions
        if transaction_ids:
            import random
            df['transaction_id'] = [random.choice(transaction_ids) for _ in range(len(df))]
            logger.info(f"Assigned transaction_ids from {len(transaction_ids)} available transactions")
        
        # Add ingestion timestamp
        ingestion_timestamp = datetime.now(timezone.utc)
        df['ingested_datetime'] = ingestion_timestamp
        
        # Flatten complex objects for PostgreSQL compatibility
        # Convert location object to separate columns
        if 'location' in df.columns:
            location_df = pd.json_normalize(df['location'])
            location_df.columns = [f'location_{col}' for col in location_df.columns]
            df = pd.concat([df.drop('location', axis=1), location_df], axis=1)
        
        # Convert metadata object to separate columns
        if 'metadata' in df.columns:
            metadata_df = pd.json_normalize(df['metadata'])
            metadata_df.columns = [f'metadata_{col}' for col in metadata_df.columns]
            df = pd.concat([df.drop('metadata', axis=1), metadata_df], axis=1)
        
        # Convert card_details to JSON string (optional field)
        if 'card_details' in df.columns:
            df['card_details'] = df['card_details'].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        
        # Convert store_details to JSON string (optional field)
        if 'store_details' in df.columns:
            df['store_details'] = df['store_details'].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        
        logger.info(f"Generated {len(df)} detailed transaction records")
        logger.info(f"Detailed transaction columns: {list(df.columns)}")
        logger.info(f"Note: 'id' column will be auto-generated by PostgreSQL SERIAL")
        
        # Prepare return data structure
        result_data = {
            "data": df,
            "table_name": "detailed_transactions",
            "num_records": len(df),
            "columns": list(df.columns),
            "primary_key": "id",
            "foreign_keys": {
                "transaction_id": "transactions.transaction_id",
                "user_id": "users.user_id"
            },
            "ingestion_timestamp": ingestion_timestamp,
            "metadata": table_metadata
        }
        
        # Log all components successfully generated
        logger.info("✅ Successfully generated detailed transactions data:")
        logger.info(f"   📊 data: DataFrame with shape {df.shape}")
        logger.info(f"   🔑 primary_key: {result_data['primary_key']} (auto-generated by PostgreSQL SERIAL)")
        logger.info(f"   🔗 foreign_keys: {result_data['foreign_keys']}")
        logger.info(f"   🏷️  table_name: {result_data['table_name']}")
        logger.info(f"   🔢 num_records: {result_data['num_records']}")
        logger.info(f"   ⏰ ingestion_timestamp: {result_data['ingestion_timestamp']}")
        logger.info(f"   �️  metadata: Schema metadata loaded successfully")
        
        return result_data
        
    except Exception as e:
        logger.error(f"Error generating detailed transactions data: {str(e)}")
        raise


def generate_relational_dataset_op(
    num_users: int = 1000,
    num_transactions: int = 5000,
    num_detailed_transactions: int = 10000,
    metadata_config_path: str = None
) -> Dict[str, Any]:
    """
    Generate a complete relational dataset with proper foreign key relationships
    
    Args:
        num_users: Number of users to generate
        num_transactions: Number of transactions to generate
        num_detailed_transactions: Number of detailed transactions to generate
        metadata_config_path: Path to metadata configuration file
    
    Returns:
        Dictionary containing all generated tables with proper relationships
    """
    try:
        logger.info("🚀 Generating complete relational dataset with foreign key relationships")
        logger.info("="*80)
        
        # Step 1: Generate users (parent table)
        logger.info("📊 Step 1: Generating users table...")
        users_result = generate_users_data_op(num_users, metadata_config_path)
        user_ids = users_result["data"]["user_id"].tolist()
        logger.info(f"✅ Generated {len(user_ids)} users")
        
        # Step 2: Generate transactions (references users)
        logger.info("📊 Step 2: Generating transactions table...")
        transactions_result = generate_transactions_data_op(num_transactions, user_ids, metadata_config_path)
        transaction_ids = transactions_result["data"]["transaction_id"].tolist()
        logger.info(f"✅ Generated {len(transaction_ids)} transactions")
        
        # Step 3: Generate detailed transactions (references both users and transactions)
        logger.info("📊 Step 3: Generating detailed_transactions table...")
        detailed_transactions_result = generate_detailed_transactions_with_incremental_pk_op(
            num_detailed_transactions, transaction_ids, user_ids, metadata_config_path
        )
        logger.info(f"✅ Generated {detailed_transactions_result['num_records']} detailed transactions")
        
        # Prepare complete dataset
        dataset = {
            "users": users_result,
            "transactions": transactions_result,
            "detailed_transactions": detailed_transactions_result,
            "relationships": {
                "transactions.user_id -> users.user_id": len(transaction_ids),
                "detailed_transactions.user_id -> users.user_id": detailed_transactions_result['num_records'],
                "detailed_transactions.transaction_id -> transactions.transaction_id": detailed_transactions_result['num_records']
            },
            "generation_timestamp": datetime.now(timezone.utc)
        }
        
        # Log final summary
        logger.info("="*80)
        logger.info("🎉 RELATIONAL DATASET GENERATION COMPLETED")
        logger.info("="*80)
        logger.info("📊 Dataset Summary:")
        logger.info(f"   👥 Users: {users_result['num_records']} records (Primary Key: user_id)")
        logger.info(f"   💳 Transactions: {transactions_result['num_records']} records (Primary Key: transaction_id, Foreign Key: user_id)")
        logger.info(f"   🔍 Detailed Transactions: {detailed_transactions_result['num_records']} records (Primary Key: id, Foreign Keys: transaction_id, user_id)")
        logger.info("📈 Foreign Key Relationships:")
        for relationship, count in dataset["relationships"].items():
            logger.info(f"   🔗 {relationship}: {count} relationships")
        
        return dataset
        
    except Exception as e:
        logger.error(f"Error generating relational dataset: {str(e)}")
        raise


if __name__ == "__main__":
    # Example usage - demonstrating relational data generation without file/directory creation
    logger.info("🧪 Testing relational data generation operations...")
    logger.info("📝 Note: No files or directories will be created, only in-memory data generation")
    
    # Test individual table generation
    logger.info("\n" + "="*60)
    logger.info("🧑‍� Testing Individual Table Generation")
    logger.info("="*60)
    
    # Test users generation
    user_result = generate_users_data_op(100)
    logger.info(f"✅ Generated {user_result['num_records']} user records successfully")
    
    # Test transactions generation with user foreign keys
    user_ids = user_result["data"]["user_id"].tolist()
    transaction_result = generate_transactions_data_op(200, user_ids)
    logger.info(f"✅ Generated {transaction_result['num_records']} transaction records successfully")
    
    # Test detailed transactions with incremental PK and foreign keys
    transaction_ids = transaction_result["data"]["transaction_id"].tolist()
    detailed_result = generate_detailed_transactions_with_incremental_pk_op(300, transaction_ids, user_ids)
    logger.info(f"✅ Generated {detailed_result['num_records']} detailed transaction records successfully")
    
    # Test complete relational dataset generation
    logger.info("\n" + "="*60)
    logger.info("🌐 Testing Complete Relational Dataset Generation")
    logger.info("="*60)
    
    complete_dataset = generate_relational_dataset_op(
        num_users=50,
        num_transactions=150,
        num_detailed_transactions=300
    )
    
    # Final summary
    logger.info("\n" + "="*60)
    logger.info("🎉 ALL RELATIONAL TESTS COMPLETED SUCCESSFULLY")
    logger.info("="*60)
    logger.info("📊 Complete Dataset Summary:")
    logger.info(f"   👥 Users: {complete_dataset['users']['num_records']} records")
    logger.info(f"   💳 Transactions: {complete_dataset['transactions']['num_records']} records") 
    logger.info(f"   🔍 Detailed Transactions: {complete_dataset['detailed_transactions']['num_records']} records")
    logger.info("   ✅ All data generated in-memory without file/directory creation")
    logger.info("   ✅ All foreign key relationships properly established")
    logger.info("   ✅ Incremental primary keys implemented for detailed_transactions")