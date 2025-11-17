-- Drop existing table if exists
DROP TABLE IF EXISTS streaming.user_alerts CASCADE;

-- Create alerts table to store high-value user violations
-- user_id is PRIMARY KEY to allow Flink UPSERT (one alert per user, updated as transactions accumulate)
CREATE TABLE streaming.user_alerts (
    user_id UUID PRIMARY KEY,
    total_amount NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    window_days INTEGER NOT NULL,
    threshold_amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Foreign key constraint to streaming.users
    CONSTRAINT fk_user_alerts_user_id 
        FOREIGN KEY (user_id) 
        REFERENCES streaming.users(user_id)
        ON DELETE CASCADE
);

-- Create indexes for faster queries
CREATE INDEX idx_user_alerts_detected_at ON streaming.user_alerts(detected_at);
CREATE INDEX idx_user_alerts_severity ON streaming.user_alerts(severity);
CREATE INDEX idx_user_alerts_total_amount ON streaming.user_alerts(total_amount);

-- Add comments
COMMENT ON TABLE streaming.user_alerts IS 'Stores alerts for users who exceed transaction thresholds - one row per user (upserted by Flink)';
COMMENT ON COLUMN streaming.user_alerts.user_id IS 'Primary key - unique user identifier';
COMMENT ON COLUMN streaming.user_alerts.updated_at IS 'Timestamp when the alert was last updated';
