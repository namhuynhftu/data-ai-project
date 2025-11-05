-- ==============================================
-- Database and User Administration Setup
-- ==============================================

-- Create the streaming_db database if it doesn't exist
-- Note: CREATE DATABASE IF NOT EXISTS is not standard PostgreSQL syntax
-- We'll use conditional logic instead
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'streaming_db') THEN
        CREATE DATABASE streaming_db;
    END IF;
END $$;

-- Connect to the streaming_db database
\c streaming_db;

-- ==============================================
-- Grant Admin Access to User
-- ==============================================

-- Grant all privileges on database
GRANT ALL PRIVILEGES ON DATABASE streaming_db TO "user";

-- Create schema for organizing tables
CREATE SCHEMA IF NOT EXISTS streaming;

-- Grant all privileges on schemas
GRANT ALL PRIVILEGES ON SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON SCHEMA public TO "user";

-- Grant all privileges on all tables in schemas (existing and future)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "user";

-- Grant all privileges on all sequences in schemas (existing and future)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "user";

-- Grant all privileges on all functions in schemas (existing and future)
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO "user";

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA streaming GRANT ALL ON TABLES TO "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "user";

ALTER DEFAULT PRIVILEGES IN SCHEMA streaming GRANT ALL ON SEQUENCES TO "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "user";

ALTER DEFAULT PRIVILEGES IN SCHEMA streaming GRANT ALL ON FUNCTIONS TO "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO "user";

-- Grant usage on schemas
GRANT USAGE ON SCHEMA streaming TO "user";
GRANT USAGE ON SCHEMA public TO "user";

-- Grant create privileges on schemas
GRANT CREATE ON SCHEMA streaming TO "user";
GRANT CREATE ON SCHEMA public TO "user";

-- Set search path to include our schema
SET search_path TO streaming, public;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================================
-- Users Table (Primary table)
-- ==============================================

CREATE TABLE IF NOT EXISTS streaming.users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    address VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingested_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT users_email_unique UNIQUE (email)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON streaming.users(email);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON streaming.users(created_at);

-- Add comments
COMMENT ON TABLE streaming.users IS 'User account information and demographics';
COMMENT ON COLUMN streaming.users.user_id IS 'Unique user identifier (UUID)';
COMMENT ON COLUMN streaming.users.name IS 'User''s full name';
COMMENT ON COLUMN streaming.users.email IS 'User''s email address';
COMMENT ON COLUMN streaming.users.address IS 'User''s physical address';
COMMENT ON COLUMN streaming.users.created_at IS 'Account creation timestamp';

-- ==============================================
-- Basic Transactions Table
-- ==============================================

CREATE TABLE IF NOT EXISTS streaming.transactions (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingested_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    CONSTRAINT fk_transactions_user_id 
        FOREIGN KEY (user_id) REFERENCES streaming.users(user_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON streaming.transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON streaming.transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON streaming.transactions(currency);

-- Add comments
COMMENT ON TABLE streaming.transactions IS 'Basic transaction records';
COMMENT ON COLUMN streaming.transactions.transaction_id IS 'Unique transaction identifier (UUID)';
COMMENT ON COLUMN streaming.transactions.user_id IS 'Associated user identifier';
COMMENT ON COLUMN streaming.transactions.amount IS 'Transaction amount';
COMMENT ON COLUMN streaming.transactions.currency IS 'Currency code (ISO 4217)';
COMMENT ON COLUMN streaming.transactions.timestamp IS 'Transaction timestamp';

-- ==============================================
-- Detailed Transactions Table (with incremental PK)
-- ==============================================

CREATE TABLE IF NOT EXISTS streaming.detailed_transactions (
    id SERIAL PRIMARY KEY,  -- Incremental primary key
    transaction_id UUID NOT NULL,
    user_id UUID NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(50),
    description VARCHAR(500),
    status VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- JSONB columns for complex objects
    card_details JSONB,
    store_details JSONB,
    
    -- Flattened location columns
    location_city VARCHAR(100),
    location_state VARCHAR(100),
    location_country VARCHAR(100),
    location_latitude FLOAT,
    location_longitude FLOAT,
    
    -- Flattened metadata columns
    metadata_ip_address VARCHAR(45),
    metadata_user_agent VARCHAR(500),
    metadata_device_type VARCHAR(20),
    metadata_channel VARCHAR(20),
    metadata_reference_number VARCHAR(20),
    metadata_fee_amount DECIMAL(5,2),
    
    -- Ingestion timestamp
    ingested_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    CONSTRAINT fk_detailed_transactions_transaction_id 
        FOREIGN KEY (transaction_id) REFERENCES streaming.transactions(transaction_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    
    CONSTRAINT fk_detailed_transactions_user_id 
        FOREIGN KEY (user_id) REFERENCES streaming.users(user_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Check constraints
    CONSTRAINT chk_transaction_type 
        CHECK (transaction_type IN ('purchase', 'refund', 'transfer', 'withdrawal', 'deposit', 'payment')),
    
    CONSTRAINT chk_payment_method 
        CHECK (payment_method IN ('credit_card', 'debit_card', 'bank_transfer', 'paypal', 'apple_pay', 'google_pay', 'cash')),
    
    CONSTRAINT chk_merchant_category 
        CHECK (merchant_category IN ('grocery', 'restaurant', 'gas_station', 'retail', 'online', 'entertainment', 'travel', 'healthcare', 'education', 'utilities')),
    
    CONSTRAINT chk_status 
        CHECK (status IN ('completed', 'pending', 'failed', 'cancelled', 'processing')),
    
    CONSTRAINT chk_device_type 
        CHECK (metadata_device_type IN ('mobile', 'desktop', 'tablet')),
    
    CONSTRAINT chk_channel 
        CHECK (metadata_channel IN ('online', 'mobile_app', 'in_store', 'atm', 'phone'))
);

-- Create indexes for detailed transactions
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_transaction_id ON streaming.detailed_transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_user_id ON streaming.detailed_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_timestamp ON streaming.detailed_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_type ON streaming.detailed_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_status ON streaming.detailed_transactions(status);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_merchant ON streaming.detailed_transactions(merchant_name);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_category ON streaming.detailed_transactions(merchant_category);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_payment_method ON streaming.detailed_transactions(payment_method);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_user_timestamp ON streaming.detailed_transactions(user_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_detailed_transactions_type_status ON streaming.detailed_transactions(transaction_type, status);

-- Add comments
COMMENT ON TABLE streaming.detailed_transactions IS 'Comprehensive transaction records with metadata and incremental primary key';
COMMENT ON COLUMN streaming.detailed_transactions.id IS 'Incremental primary key';
COMMENT ON COLUMN streaming.detailed_transactions.transaction_id IS 'Reference to transaction ID';
COMMENT ON COLUMN streaming.detailed_transactions.user_id IS 'Associated user identifier';
COMMENT ON COLUMN streaming.detailed_transactions.amount IS 'Transaction amount (negative for debits)';
COMMENT ON COLUMN streaming.detailed_transactions.currency IS 'Currency code (ISO 4217)';
COMMENT ON COLUMN streaming.detailed_transactions.transaction_type IS 'Type of transaction';
COMMENT ON COLUMN streaming.detailed_transactions.payment_method IS 'Payment method used';
COMMENT ON COLUMN streaming.detailed_transactions.merchant_name IS 'Merchant or company name';
COMMENT ON COLUMN streaming.detailed_transactions.merchant_category IS 'Merchant category';
COMMENT ON COLUMN streaming.detailed_transactions.description IS 'Transaction description';
COMMENT ON COLUMN streaming.detailed_transactions.status IS 'Transaction status';
COMMENT ON COLUMN streaming.detailed_transactions.timestamp IS 'Transaction timestamp';
COMMENT ON COLUMN streaming.detailed_transactions.card_details IS 'Card payment details as JSONB';
COMMENT ON COLUMN streaming.detailed_transactions.store_details IS 'Store details as JSONB';

-- ==============================================
-- Function to update timestamp on record update
-- ==============================================
CREATE OR REPLACE FUNCTION streaming.update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.timestamp = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==============================================
-- Final Admin Access Grants (Post Table Creation)
-- ==============================================

-- Grant all privileges on all newly created tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "user";

-- Grant all privileges on all sequences (for SERIAL columns)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA streaming TO "user";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "user";

-- Grant execute privileges on all functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA streaming TO "user";
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO "user";

-- Make the user the owner of all objects in streaming schema
DO $$
DECLARE
    r RECORD;
BEGIN
    -- Change ownership of all tables
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'streaming')
    LOOP
        EXECUTE 'ALTER TABLE streaming.' || quote_ident(r.tablename) || ' OWNER TO "user"';
    END LOOP;
    
    -- Change ownership of all sequences
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'streaming')
    LOOP
        EXECUTE 'ALTER SEQUENCE streaming.' || quote_ident(r.sequence_name) || ' OWNER TO "user"';
    END LOOP;
    
    -- Change ownership of all functions (with error handling)
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'streaming' AND routine_type = 'FUNCTION')
    LOOP
        BEGIN
            EXECUTE 'ALTER FUNCTION streaming.' || quote_ident(r.routine_name) || '() OWNER TO "user"';
        EXCEPTION
            WHEN OTHERS THEN
                -- Log the error but continue
                RAISE NOTICE 'Could not change ownership of function %: %', r.routine_name, SQLERRM;
        END;
    END LOOP;
END $$;

-- Change ownership of the schema itself
ALTER SCHEMA streaming OWNER TO "user";

-- Grant CREATE and USAGE on database level
GRANT CREATE, CONNECT ON DATABASE streaming_db TO "user";

-- Show confirmation message
SELECT 'Relational schema setup completed with foreign key relationships' AS status;