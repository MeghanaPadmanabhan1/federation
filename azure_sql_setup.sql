-- ============================================================================
-- Azure SQL Database Setup Script for Lakehouse Federation Demo
-- ============================================================================
-- Purpose: Create sample customer holdings data to demonstrate federation
-- Run this script in your Azure SQL Database
-- ============================================================================

-- Create customer holdings table
DROP TABLE IF EXISTS dbo.customer_holdings;
GO

CREATE TABLE dbo.customer_holdings (
    holding_id INT PRIMARY KEY IDENTITY(1,1),
    customer_id INT NOT NULL,
    ticker_symbol VARCHAR(10) NOT NULL,
    shares_held DECIMAL(18,2) NOT NULL,
    cost_basis DECIMAL(18,2) NOT NULL,
    purchase_date DATE NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    last_updated DATETIME2 DEFAULT GETDATE()
);
GO

-- Create indexes for better query performance
CREATE INDEX IX_customer_holdings_customer_id
    ON dbo.customer_holdings(customer_id);

CREATE INDEX IX_customer_holdings_ticker
    ON dbo.customer_holdings(ticker_symbol);

CREATE INDEX IX_customer_holdings_purchase_date
    ON dbo.customer_holdings(purchase_date);
GO

-- Insert sample customer holdings data
-- These tickers should align with FactSet data for meaningful joins
INSERT INTO dbo.customer_holdings (customer_id, ticker_symbol, shares_held, cost_basis, purchase_date, account_type, account_number)
VALUES
    -- Customer 1001 - Diversified tech portfolio
    (1001, 'AAPL', 150.00, 142.50, '2023-01-15', 'Brokerage', 'BRK-1001-001'),
    (1001, 'MSFT', 85.00, 275.30, '2023-02-20', 'IRA', 'IRA-1001-001'),
    (1001, 'GOOGL', 60.00, 98.75, '2023-03-10', 'Brokerage', 'BRK-1001-001'),
    (1001, 'AMZN', 45.00, 88.20, '2023-04-05', '401k', '401-1001-001'),
    (1001, 'META', 90.00, 165.50, '2023-05-12', 'Brokerage', 'BRK-1001-001'),

    -- Customer 1002 - Tech and automotive focus
    (1002, 'TSLA', 120.00, 185.75, '2023-01-20', 'Brokerage', 'BRK-1002-001'),
    (1002, 'NVDA', 200.00, 145.25, '2023-02-15', 'Brokerage', 'BRK-1002-001'),
    (1002, 'AMD', 175.00, 78.90, '2023-03-22', 'IRA', 'IRA-1002-001'),
    (1002, 'MSFT', 95.00, 280.00, '2023-04-18', 'Brokerage', 'BRK-1002-001'),

    -- Customer 1003 - Financial services focus
    (1003, 'JPM', 180.00, 125.40, '2023-02-01', 'Brokerage', 'BRK-1003-001'),
    (1003, 'GS', 75.00, 315.80, '2023-02-10', 'Brokerage', 'BRK-1003-001'),
    (1003, 'BAC', 300.00, 28.50, '2023-03-05', 'IRA', 'IRA-1003-001'),
    (1003, 'WFC', 250.00, 35.75, '2023-03-15', 'Brokerage', 'BRK-1003-001'),
    (1003, 'C', 200.00, 42.30, '2023-04-01', '401k', '401-1003-001'),

    -- Customer 1004 - Healthcare portfolio
    (1004, 'JNJ', 140.00, 155.20, '2023-01-10', 'IRA', 'IRA-1004-001'),
    (1004, 'UNH', 65.00, 475.50, '2023-01-25', 'Brokerage', 'BRK-1004-001'),
    (1004, 'PFE', 320.00, 32.15, '2023-02-14', 'Brokerage', 'BRK-1004-001'),
    (1004, 'ABBV', 110.00, 138.90, '2023-03-08', 'IRA', 'IRA-1004-001'),

    -- Customer 1005 - Consumer goods and retail
    (1005, 'WMT', 200.00, 145.80, '2023-01-18', 'Brokerage', 'BRK-1005-001'),
    (1005, 'HD', 90.00, 285.40, '2023-02-22', 'Brokerage', 'BRK-1005-001'),
    (1005, 'PG', 175.00, 135.60, '2023-03-12', 'IRA', 'IRA-1005-001'),
    (1005, 'KO', 280.00, 58.25, '2023-04-08', 'Brokerage', 'BRK-1005-001'),
    (1005, 'NKE', 130.00, 95.75, '2023-05-03', 'Brokerage', 'BRK-1005-001'),

    -- Customer 1006 - Energy and industrials
    (1006, 'XOM', 220.00, 95.40, '2023-02-05', 'Brokerage', 'BRK-1006-001'),
    (1006, 'CVX', 185.00, 142.70, '2023-02-18', 'Brokerage', 'BRK-1006-001'),
    (1006, 'CAT', 95.00, 215.30, '2023-03-20', 'IRA', 'IRA-1006-001'),
    (1006, 'BA', 75.00, 178.50, '2023-04-12', 'Brokerage', 'BRK-1006-001'),

    -- Customer 1007 - Mixed portfolio with recent purchases
    (1007, 'AAPL', 110.00, 168.90, '2024-01-15', 'Brokerage', 'BRK-1007-001'),
    (1007, 'TSLA', 65.00, 245.30, '2024-01-22', 'IRA', 'IRA-1007-001'),
    (1007, 'NVDA', 150.00, 425.75, '2024-02-08', 'Brokerage', 'BRK-1007-001'),
    (1007, 'AMZN', 55.00, 152.80, '2024-02-20', 'Brokerage', 'BRK-1007-001');
GO

-- Create customer profile table (additional context)
DROP TABLE IF EXISTS dbo.customer_profiles;
GO

CREATE TABLE dbo.customer_profiles (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    risk_tolerance VARCHAR(20),
    account_created_date DATE NOT NULL,
    total_account_value DECIMAL(18,2),
    is_accredited_investor BIT DEFAULT 0
);
GO

INSERT INTO dbo.customer_profiles (customer_id, customer_name, email, risk_tolerance, account_created_date, total_account_value, is_accredited_investor)
VALUES
    (1001, 'John Anderson', 'john.anderson@email.com', 'Moderate', '2022-06-15', 125000.00, 1),
    (1002, 'Sarah Chen', 'sarah.chen@email.com', 'Aggressive', '2022-08-20', 185000.00, 1),
    (1003, 'Michael Rodriguez', 'michael.r@email.com', 'Conservative', '2022-09-10', 245000.00, 1),
    (1004, 'Emily Watson', 'emily.watson@email.com', 'Moderate', '2022-11-05', 95000.00, 0),
    (1005, 'David Kim', 'david.kim@email.com', 'Moderate', '2023-01-12', 165000.00, 1),
    (1006, 'Lisa Martinez', 'lisa.m@email.com', 'Conservative', '2023-02-28', 210000.00, 1),
    (1007, 'James Wilson', 'james.wilson@email.com', 'Aggressive', '2023-12-10', 225000.00, 1);
GO

-- Create account transactions table (for additional analytics)
DROP TABLE IF EXISTS dbo.account_transactions;
GO

CREATE TABLE dbo.account_transactions (
    transaction_id INT PRIMARY KEY IDENTITY(1,1),
    customer_id INT NOT NULL,
    ticker_symbol VARCHAR(10) NOT NULL,
    transaction_type VARCHAR(10) NOT NULL, -- BUY, SELL
    shares DECIMAL(18,2) NOT NULL,
    price_per_share DECIMAL(18,2) NOT NULL,
    transaction_date DATETIME2 NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    CONSTRAINT FK_transactions_customer FOREIGN KEY (customer_id)
        REFERENCES dbo.customer_profiles(customer_id)
);
GO

CREATE INDEX IX_account_transactions_customer_id
    ON dbo.account_transactions(customer_id);

CREATE INDEX IX_account_transactions_date
    ON dbo.account_transactions(transaction_date);
GO

-- Insert sample transactions
INSERT INTO dbo.account_transactions (customer_id, ticker_symbol, transaction_type, shares, price_per_share, transaction_date, account_number)
VALUES
    (1001, 'AAPL', 'BUY', 150.00, 142.50, '2023-01-15 10:30:00', 'BRK-1001-001'),
    (1001, 'MSFT', 'BUY', 85.00, 275.30, '2023-02-20 14:15:00', 'IRA-1001-001'),
    (1002, 'TSLA', 'BUY', 120.00, 185.75, '2023-01-20 09:45:00', 'BRK-1002-001'),
    (1002, 'NVDA', 'BUY', 200.00, 145.25, '2023-02-15 11:20:00', 'BRK-1002-001'),
    (1003, 'JPM', 'BUY', 180.00, 125.40, '2023-02-01 13:00:00', 'BRK-1003-001'),
    (1007, 'NVDA', 'BUY', 150.00, 425.75, '2024-02-08 10:00:00', 'BRK-1007-001'),
    (1007, 'NVDA', 'SELL', 25.00, 485.20, '2024-02-25 15:30:00', 'BRK-1007-001');
GO

-- Create views for easier querying
CREATE OR REPLACE VIEW dbo.vw_customer_portfolio_summary AS
SELECT
    cp.customer_id,
    cp.customer_name,
    cp.risk_tolerance,
    COUNT(ch.holding_id) AS num_positions,
    SUM(ch.shares_held * ch.cost_basis) AS total_portfolio_value,
    COUNT(DISTINCT ch.ticker_symbol) AS unique_holdings,
    MIN(ch.purchase_date) AS earliest_purchase,
    MAX(ch.purchase_date) AS latest_purchase
FROM dbo.customer_profiles cp
LEFT JOIN dbo.customer_holdings ch ON cp.customer_id = ch.customer_id
GROUP BY cp.customer_id, cp.customer_name, cp.risk_tolerance;
GO

-- Verify data
SELECT 'Customer Holdings' AS TableName, COUNT(*) AS RowCount FROM dbo.customer_holdings
UNION ALL
SELECT 'Customer Profiles', COUNT(*) FROM dbo.customer_profiles
UNION ALL
SELECT 'Account Transactions', COUNT(*) FROM dbo.account_transactions;
GO

-- Sample queries to verify
SELECT * FROM dbo.vw_customer_portfolio_summary ORDER BY total_portfolio_value DESC;
GO

SELECT
    ticker_symbol,
    COUNT(*) AS num_customers,
    SUM(shares_held) AS total_shares,
    AVG(cost_basis) AS avg_cost_basis
FROM dbo.customer_holdings
GROUP BY ticker_symbol
ORDER BY num_customers DESC;
GO

-- Grant permissions (adjust as needed for your security model)
-- GRANT SELECT ON dbo.customer_holdings TO [your_databricks_user];
-- GRANT SELECT ON dbo.customer_profiles TO [your_databricks_user];
-- GRANT SELECT ON dbo.account_transactions TO [your_databricks_user];
-- GRANT SELECT ON dbo.vw_customer_portfolio_summary TO [your_databricks_user];
-- GO

PRINT 'Azure SQL Database setup complete!';
PRINT 'Tables created: customer_holdings, customer_profiles, account_transactions';
PRINT 'View created: vw_customer_portfolio_summary';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Update Databricks notebook with your Azure SQL connection details';
PRINT '2. Create connection and foreign catalog in Databricks';
PRINT '3. Run the federation demo notebook';
GO
