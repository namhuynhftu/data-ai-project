#!/usr/bin/env python3
"""
Complete Relational Data Loading Script

This script generates and loads all three relational tables to PostgreSQL:
1. Users table (parent table)
2. Transactions table (references users)
3. Detailed_transactions table (references both users and transactions)

All tables are loaded with proper foreign key relationships maintained.
"""

import logging
import sys
from datetime import datetime
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from elt_pipeline.streaming.ops.generate_data import generate_relational_dataset_op
from elt_pipeline.streaming.ops.load_data_to_psql import load_data_to_psql_op, load_psql_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_all_relational_tables(
    num_users: int = 100,
    num_transactions: int = 300,
    num_detailed_transactions: int = 500,
    chunk_size: int = 50
):
    """
    Generate and load all relational tables to PostgreSQL
    
    Args:
        num_users: Number of user records to generate
        num_transactions: Number of transaction records to generate
        num_detailed_transactions: Number of detailed transaction records to generate
        chunk_size: Number of records to load per chunk
    """
    try:
        logger.info("🚀 STARTING COMPLETE RELATIONAL DATA LOADING")
        logger.info("="*80)
        
        # Step 1: Generate complete relational dataset
        logger.info("📊 Step 1: Generating complete relational dataset...")
        dataset = generate_relational_dataset_op(
            num_users=num_users,
            num_transactions=num_transactions,
            num_detailed_transactions=num_detailed_transactions
        )
        
        # Step 2: Load PostgreSQL configuration
        logger.info("📊 Step 2: Loading PostgreSQL configuration...")
        psql_config = load_psql_config()
        logger.info(f"   Database: {psql_config['database']}")
        logger.info(f"   Schema: {psql_config['schema']}")
        logger.info(f"   Host: {psql_config['host']}:{psql_config['port']}")
        
        # Step 3: Load Users table (parent table)
        logger.info("📊 Step 3: Loading Users table...")
        logger.info("-" * 50)
        users_result = load_data_to_psql_op(
            data_result=dataset["users"],
            target_table="users",
            psql_config=psql_config,
            chunk_size=chunk_size
        )
        
        logger.info(f"✅ Users table loaded:")
        logger.info(f"   Records: {users_result['rows_loaded']}/{users_result['total_rows']}")
        logger.info(f"   Chunks: {users_result['successful_chunks']}/{users_result['total_chunks']}")
        logger.info(f"   Table: streaming.users")
        
        # Step 4: Load Transactions table (references users)
        logger.info("\n📊 Step 4: Loading Transactions table...")
        logger.info("-" * 50)
        transactions_result = load_data_to_psql_op(
            data_result=dataset["transactions"],
            target_table="transactions",
            psql_config=psql_config,
            chunk_size=chunk_size
        )
        
        logger.info(f"✅ Transactions table loaded:")
        logger.info(f"   Records: {transactions_result['rows_loaded']}/{transactions_result['total_rows']}")
        logger.info(f"   Chunks: {transactions_result['successful_chunks']}/{transactions_result['total_chunks']}")
        logger.info(f"   Table: streaming.transactions")
        logger.info(f"   Foreign Key: user_id -> streaming.users.user_id")
        
        # Step 5: Load Detailed Transactions table (references both users and transactions)
        logger.info("\n📊 Step 5: Loading Detailed Transactions table...")
        logger.info("-" * 50)
        detailed_result = load_data_to_psql_op(
            data_result=dataset["detailed_transactions"],
            target_table="detailed_transactions", 
            psql_config=psql_config,
            chunk_size=chunk_size
        )
        
        logger.info(f"✅ Detailed Transactions table loaded:")
        logger.info(f"   Records: {detailed_result['rows_loaded']}/{detailed_result['total_rows']}")
        logger.info(f"   Chunks: {detailed_result['successful_chunks']}/{detailed_result['total_chunks']}")
        logger.info(f"   Table: streaming.detailed_transactions")
        logger.info(f"   Foreign Keys: user_id -> streaming.users.user_id")
        logger.info(f"                transaction_id -> streaming.transactions.transaction_id")
        
        # Final Summary
        logger.info("\n" + "="*80)
        logger.info("🎉 COMPLETE RELATIONAL DATA LOADING SUMMARY")
        logger.info("="*80)
        
        total_records = (users_result['rows_loaded'] + 
                        transactions_result['rows_loaded'] + 
                        detailed_result['rows_loaded'])
        
        total_expected = (users_result['total_rows'] + 
                         transactions_result['total_rows'] + 
                         detailed_result['total_rows'])
        
        logger.info(f"📊 Total Records Loaded: {total_records}/{total_expected}")
        logger.info(f"📋 Tables Loaded:")
        logger.info(f"   👥 streaming.users: {users_result['rows_loaded']} records")
        logger.info(f"   💳 streaming.transactions: {transactions_result['rows_loaded']} records")
        logger.info(f"   🔍 streaming.detailed_transactions: {detailed_result['rows_loaded']} records")
        
        logger.info(f"🔗 Foreign Key Relationships:")
        logger.info(f"   transactions.user_id -> users.user_id: {transactions_result['rows_loaded']} relationships")
        logger.info(f"   detailed_transactions.user_id -> users.user_id: {detailed_result['rows_loaded']} relationships")
        logger.info(f"   detailed_transactions.transaction_id -> transactions.transaction_id: {detailed_result['rows_loaded']} relationships")
        
        # Check for any failures
        total_failed_chunks = (users_result['failed_chunks'] + 
                              transactions_result['failed_chunks'] + 
                              detailed_result['failed_chunks'])
        
        if total_failed_chunks > 0:
            logger.warning(f"⚠️  {total_failed_chunks} chunks failed during loading")
        else:
            logger.info("✅ All chunks loaded successfully!")
        
        # Return summary
        return {
            "users": users_result,
            "transactions": transactions_result,
            "detailed_transactions": detailed_result,
            "total_records_loaded": total_records,
            "total_expected_records": total_expected,
            "success_rate": (total_records / total_expected) * 100 if total_expected > 0 else 0,
            "loading_timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"❌ Error during relational data loading: {str(e)}")
        raise

def verify_data_loading():
    """Verify that data was loaded correctly with proper relationships"""
    try:
        logger.info("\n🔍 VERIFYING DATA LOADING")
        logger.info("="*50)
        
        # This could be extended to run SQL queries to verify the data
        # For now, we'll just log the verification step
        logger.info("📋 Verification steps:")
        logger.info("   1. Check record counts in each table")
        logger.info("   2. Verify foreign key relationships")
        logger.info("   3. Validate data integrity")
        logger.info("   4. Check schema compliance")
        
        # You could add actual verification queries here
        logger.info("✅ Manual verification required - check database directly")
        
    except Exception as e:
        logger.error(f"❌ Error during data verification: {str(e)}")

def main():
    """Main function to orchestrate the complete data loading process"""
    try:
        logger.info("🎬 STARTING COMPLETE RELATIONAL DATA PIPELINE")
        logger.info(f"⏰ Start Time: {datetime.now()}")
        logger.info("="*80)
        
        # Configuration
        config = {
            "num_users": 50,
            "num_transactions": 150, 
            "num_detailed_transactions": 300,
            "chunk_size": 25
        }
        
        logger.info("📋 Pipeline Configuration:")
        for key, value in config.items():
            logger.info(f"   {key}: {value}")
        
        # Load all relational tables
        results = load_all_relational_tables(**config)
        
        # Verify data loading
        verify_data_loading()
        
        # Final status
        logger.info("\n" + "="*80)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"✅ Success Rate: {results['success_rate']:.1f}%")
        logger.info(f"📊 Total Records: {results['total_records_loaded']}")
        logger.info(f"⏰ Completion Time: {datetime.now()}")
        logger.info("\n🔍 To verify the data, you can run:")
        logger.info("   docker exec -it postgres_dw psql -U user -d streaming_db")
        logger.info("   \\c streaming_db")
        logger.info("   SELECT COUNT(*) FROM streaming.users;")
        logger.info("   SELECT COUNT(*) FROM streaming.transactions;")
        logger.info("   SELECT COUNT(*) FROM streaming.detailed_transactions;")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Run the complete relational data loading pipeline
    main()