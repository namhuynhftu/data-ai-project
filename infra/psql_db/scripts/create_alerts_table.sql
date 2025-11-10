-- Create alerts table to store high-value user violations
CREATE TABLE IF NOT EXISTS streaming.user_alerts (
    alert_id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    window_days INTEGER NOT NULL,
    threshold_amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_user_alerts_user_id ON streaming.user_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_user_alerts_detected_at ON streaming.user_alerts(detected_at);
CREATE INDEX IF NOT EXISTS idx_user_alerts_severity ON streaming.user_alerts(severity);

-- Add comment
COMMENT ON TABLE streaming.user_alerts IS 'Stores alerts for users who exceed transaction thresholds';
