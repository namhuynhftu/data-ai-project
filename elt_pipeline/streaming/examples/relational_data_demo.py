#!/usr/bin/env python3
"""
Example script demonstrating relational data generation with proper foreign key relationships

This script shows how to generate:
1. Users table with user_id as primary key
2. Transactions table with transaction_id as primary key and user_id as foreign key
3. Detailed_transactions table with incremental primary key and foreign keys to both users and transactions

All relationships are properly maintained to ensure referential integrity.
"""

import logging
from elt_pipeline.streaming.ops.generate_data import (
    generate_users_data_op,
    generate_transactions_data_op,
    generate_detailed_transactions_with_incremental_pk_op,
    generate_relational_dataset_op
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_individual_table_generation():
    """Demonstrate individual table generation with proper relationships"""
    logger.info("🚀 DEMO: Individual Table Generation with Foreign Key Relationships")
    logger.info("="*80)
    
    # Step 1: Generate users (primary table)
    logger.info("Step 1: Generating users table...")
    users_data = generate_users_data_op(num_records=20)
    user_ids = users_data["data"]["user_id"].tolist()
    logger.info(f"✅ Generated {len(user_ids)} users with user_ids")
    
    # Step 2: Generate transactions (references users)
    logger.info("\nStep 2: Generating transactions table...")
    transactions_data = generate_transactions_data_op(
        num_records=50, 
        user_ids=user_ids  # Foreign key relationship
    )
    transaction_ids = transactions_data["data"]["transaction_id"].tolist()
    logger.info(f"✅ Generated {len(transaction_ids)} transactions referencing {len(user_ids)} users")
    
    # Step 3: Generate detailed transactions (references both users and transactions)
    logger.info("\nStep 3: Generating detailed_transactions table...")
    detailed_data = generate_detailed_transactions_with_incremental_pk_op(
        num_records=100,
        transaction_ids=transaction_ids,  # Foreign key to transactions
        user_ids=user_ids,               # Foreign key to users
        start_id=1                       # Incremental primary key starting from 1
    )
    logger.info(f"✅ Generated {detailed_data['num_records']} detailed transactions")
    
    # Display relationships summary
    logger.info("\n📊 RELATIONSHIP SUMMARY:")
    logger.info("="*50)
    logger.info(f"👥 Users: {users_data['num_records']} records")
    logger.info(f"   Primary Key: user_id")
    logger.info(f"💳 Transactions: {transactions_data['num_records']} records")
    logger.info(f"   Primary Key: transaction_id")
    logger.info(f"   Foreign Key: user_id -> users.user_id")
    logger.info(f"🔍 Detailed Transactions: {detailed_data['num_records']} records")
    logger.info(f"   Primary Key: id (incremental)")
    logger.info(f"   Foreign Keys: transaction_id -> transactions.transaction_id")
    logger.info(f"                user_id -> users.user_id")
    
    return {
        "users": users_data,
        "transactions": transactions_data,
        "detailed_transactions": detailed_data
    }

def demo_complete_dataset_generation():
    """Demonstrate complete relational dataset generation in one call"""
    logger.info("\n\n🌐 DEMO: Complete Relational Dataset Generation")
    logger.info("="*80)
    
    # Generate complete relational dataset
    complete_dataset = generate_relational_dataset_op(
        num_users=30,
        num_transactions=75,
        num_detailed_transactions=150
    )
    
    # Display dataset information
    logger.info("\n📈 COMPLETE DATASET INFORMATION:")
    logger.info("="*50)
    
    users_df = complete_dataset["users"]["data"]
    transactions_df = complete_dataset["transactions"]["data"]
    detailed_df = complete_dataset["detailed_transactions"]["data"]
    
    logger.info(f"👥 Users DataFrame: {users_df.shape}")
    logger.info(f"   Columns: {list(users_df.columns)}")
    logger.info(f"   Sample user_ids: {users_df['user_id'].head(3).tolist()}")
    
    logger.info(f"\n💳 Transactions DataFrame: {transactions_df.shape}")
    logger.info(f"   Columns: {list(transactions_df.columns)}")
    logger.info(f"   Sample transaction_ids: {transactions_df['transaction_id'].head(3).tolist()}")
    
    logger.info(f"\n🔍 Detailed Transactions DataFrame: {detailed_df.shape}")
    logger.info(f"   Columns: {list(detailed_df.columns)}")
    logger.info(f"   Primary key range: {detailed_df['id'].min()} to {detailed_df['id'].max()}")
    
    # Verify foreign key relationships
    logger.info(f"\n🔗 FOREIGN KEY VERIFICATION:")
    logger.info("="*40)
    
    # Check transactions.user_id references users.user_id
    user_ids_set = set(users_df['user_id'])
    transaction_user_ids = set(transactions_df['user_id'])
    valid_transaction_refs = transaction_user_ids.issubset(user_ids_set)
    logger.info(f"✅ Transactions -> Users: {valid_transaction_refs} (All transaction user_ids exist in users table)")
    
    # Check detailed_transactions.user_id references users.user_id
    detailed_user_ids = set(detailed_df['user_id'])
    valid_detailed_user_refs = detailed_user_ids.issubset(user_ids_set)
    logger.info(f"✅ Detailed Transactions -> Users: {valid_detailed_user_refs} (All detailed transaction user_ids exist in users table)")
    
    # Check detailed_transactions.transaction_id references transactions.transaction_id
    transaction_ids_set = set(transactions_df['transaction_id'])
    detailed_transaction_ids = set(detailed_df['transaction_id'])
    valid_detailed_transaction_refs = detailed_transaction_ids.issubset(transaction_ids_set)
    logger.info(f"✅ Detailed Transactions -> Transactions: {valid_detailed_transaction_refs} (All detailed transaction transaction_ids exist in transactions table)")
    
    return complete_dataset

def demo_data_inspection():
    """Demonstrate data inspection and validation"""
    logger.info("\n\n🔍 DEMO: Data Inspection and Validation")
    logger.info("="*80)
    
    # Generate small dataset for inspection
    dataset = generate_relational_dataset_op(
        num_users=5,
        num_transactions=10,
        num_detailed_transactions=15
    )
    
    users_df = dataset["users"]["data"]
    transactions_df = dataset["transactions"]["data"]
    detailed_df = dataset["detailed_transactions"]["data"]
    
    logger.info("📋 SAMPLE DATA INSPECTION:")
    logger.info("="*40)
    
    # Show sample users
    logger.info("👥 Sample Users:")
    for _, user in users_df.head(3).iterrows():
        logger.info(f"   user_id: {user['user_id'][:8]}... | name: {user['name']} | email: {user['email']}")
    
    # Show sample transactions
    logger.info("\n💳 Sample Transactions:")
    for _, txn in transactions_df.head(3).iterrows():
        logger.info(f"   transaction_id: {txn['transaction_id'][:8]}... | user_id: {txn['user_id'][:8]}... | amount: {txn['amount']} {txn['currency']}")
    
    # Show sample detailed transactions
    logger.info("\n🔍 Sample Detailed Transactions:")
    for _, detail in detailed_df.head(3).iterrows():
        logger.info(f"   id: {detail['id']} | transaction_id: {detail['transaction_id'][:8]}... | user_id: {detail['user_id'][:8]}... | amount: {detail['amount']}")

if __name__ == "__main__":
    logger.info("🎬 STARTING RELATIONAL DATA GENERATION DEMOS")
    logger.info("="*80)
    
    # Demo 1: Individual table generation
    demo_individual_table_generation()
    
    # Demo 2: Complete dataset generation
    demo_complete_dataset_generation()
    
    # Demo 3: Data inspection
    demo_data_inspection()
    
    logger.info("\n🎉 ALL DEMOS COMPLETED SUCCESSFULLY!")
    logger.info("="*80)
    logger.info("📚 Key Features Demonstrated:")
    logger.info("   ✅ Primary key generation (user_id, transaction_id, incremental id)")
    logger.info("   ✅ Foreign key relationships (user_id, transaction_id)")
    logger.info("   ✅ Referential integrity validation")
    logger.info("   ✅ Relational data generation without file creation")
    logger.info("   ✅ In-memory DataFrame operations")
    logger.info("   ✅ Metadata-driven schema handling")