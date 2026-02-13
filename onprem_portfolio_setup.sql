-- ============================================================================
-- On-Premise Portfolio Database Setup
-- ============================================================================
-- Purpose: Create sample customer portfolio data (on-premise/Azure SQL)
-- This represents the "first-party" customer data that stays on-premise
-- Run this in your on-premise SQL Server or Azure SQL Database
-- ============================================================================

-- Create customer holdings table
-- This is the core table that will be FEDERATED (queried in place, not moved)
DROP TABLE IF EXISTS dbo.customer_holdings;
GO

CREATE TABLE dbo.customer_holdings (
    holding_id INT PRIMARY KEY IDENTITY(1,1),
    customer_id INT NOT NULL,
    ticker_symbol VARCHAR(10) NOT NULL,  -- Standard ticker (MSFT, AAPL, etc.)
    shares_held DECIMAL(18,4) NOT NULL,
    cost_basis DECIMAL(18,4) NOT NULL,   -- Average cost per share
    purchase_date DATE NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    last_updated DATETIME2 DEFAULT GETDATE()
);
GO

-- Create indexes for performance
CREATE INDEX IX_customer_holdings_customer_id
    ON dbo.customer_holdings(customer_id);

CREATE INDEX IX_customer_holdings_ticker
    ON dbo.customer_holdings(ticker_symbol);

CREATE INDEX IX_customer_holdings_purchase_date
    ON dbo.customer_holdings(purchase_date);
GO

-- Insert sample portfolio holdings
-- These ticker symbols should have corresponding FactSet data
INSERT INTO dbo.customer_holdings (customer_id, ticker_symbol, shares_held, cost_basis, purchase_date, account_type, account_number)
VALUES
    -- Customer 1001 - Tech-focused investor
    (1001, 'MSFT', 250.0000, 325.5000, '2023-01-15', 'Brokerage', 'BRK-1001-001'),
    (1001, 'AAPL', 180.0000, 165.2500, '2023-02-10', 'IRA', 'IRA-1001-001'),
    (1001, 'GOOGL', 95.0000, 112.7500, '2023-03-22', 'Brokerage', 'BRK-1001-001'),
    (1001, 'NVDA', 150.0000, 285.3000, '2023-04-18', 'Brokerage', 'BRK-1001-001'),

    -- Customer 1002 - Diversified portfolio
    (1002, 'MSFT', 120.0000, 340.0000, '2023-01-20', 'Brokerage', 'BRK-1002-001'),
    (1002, 'JPM', 300.0000, 142.5000, '2023-02-15', 'IRA', 'IRA-1002-001'),
    (1002, 'JNJ', 200.0000, 158.7500, '2023-03-10', 'Brokerage', 'BRK-1002-001'),
    (1002, 'TSLA', 75.0000, 215.2500, '2023-05-08', '401k', '401-1002-001'),

    -- Customer 1003 - Financial services focus
    (1003, 'GS', 100.0000, 325.8000, '2023-02-01', 'Brokerage', 'BRK-1003-001'),
    (1003, 'BAC', 500.0000, 32.5000, '2023-02-14', 'Brokerage', 'BRK-1003-001'),
    (1003, 'WFC', 400.0000, 38.7500, '2023-03-20', 'IRA', 'IRA-1003-001'),
    (1003, 'MS', 150.0000, 85.2500, '2023-04-12', 'Brokerage', 'BRK-1003-001'),

    -- Customer 1004 - FAANG stocks
    (1004, 'META', 200.0000, 185.5000, '2023-01-25', 'Brokerage', 'BRK-1004-001'),
    (1004, 'AMZN', 120.0000, 95.7500, '2023-02-18', 'Brokerage', 'BRK-1004-001'),
    (1004, 'NFLX', 85.0000, 325.0000, '2023-03-15', 'IRA', 'IRA-1004-001'),
    (1004, 'AAPL', 220.0000, 152.2500, '2023-04-20', 'Brokerage', 'BRK-1004-001'),

    -- Customer 1005 - Blue chip stocks
    (1005, 'JNJ', 250.0000, 162.5000, '2023-01-10', 'IRA', 'IRA-1005-001'),
    (1005, 'PG', 300.0000, 142.7500, '2023-02-05', 'IRA', 'IRA-1005-001'),
    (1005, 'KO', 450.0000, 58.5000, '2023-03-12', 'Brokerage', 'BRK-1005-001'),
    (1005, 'WMT', 180.0000, 148.2500, '2023-04-08', 'Brokerage', 'BRK-1005-001'),

    -- Customer 1006 - Energy and industrials
    (1006, 'XOM', 350.0000, 98.5000, '2023-02-20', 'Brokerage', 'BRK-1006-001'),
    (1006, 'CVX', 280.0000, 155.7500, '2023-03-15', 'Brokerage', 'BRK-1006-001'),
    (1006, 'CAT', 125.0000, 225.0000, '2023-04-10', 'IRA', 'IRA-1006-001'),

    -- Customer 1007 - Recent high-tech purchases
    (1007, 'NVDA', 300.0000, 445.0000, '2024-01-15', 'Brokerage', 'BRK-1007-001'),
    (1007, 'MSFT', 180.0000, 385.5000, '2024-01-22', 'Brokerage', 'BRK-1007-001'),
    (1007, 'AMD', 400.0000, 145.7500, '2024-02-10', 'Brokerage', 'BRK-1007-001');
GO

-- Create customer profiles table
DROP TABLE IF EXISTS dbo.customer_profiles;
GO

CREATE TABLE dbo.customer_profiles (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    risk_tolerance VARCHAR(20),
    account_opened_date DATE NOT NULL,
    advisor_name VARCHAR(100),
    is_accredited_investor BIT DEFAULT 0,
    last_reviewed_date DATE
);
GO

INSERT INTO dbo.customer_profiles (customer_id, customer_name, email, phone, risk_tolerance, account_opened_date, advisor_name, is_accredited_investor, last_reviewed_date)
VALUES
    (1001, 'Robert Mitchell', 'rmitchell@email.com', '555-0101', 'Aggressive', '2022-06-15', 'Sarah Johnson', 1, '2024-01-15'),
    (1002, 'Jennifer Lee', 'jlee@email.com', '555-0102', 'Moderate', '2022-08-20', 'Michael Chen', 1, '2024-01-20'),
    (1003, 'David Rodriguez', 'drodriguez@email.com', '555-0103', 'Moderate', '2022-09-10', 'Sarah Johnson', 1, '2024-02-01'),
    (1004, 'Emily Thompson', 'ethompson@email.com', '555-0104', 'Aggressive', '2022-11-05', 'Robert Davis', 1, '2024-02-10'),
    (1005, 'William Anderson', 'wanderson@email.com', '555-0105', 'Conservative', '2023-01-12', 'Michael Chen', 0, '2024-01-25'),
    (1006, 'Lisa Martinez', 'lmartinez@email.com', '555-0106', 'Moderate', '2023-02-28', 'Sarah Johnson', 1, '2024-02-15'),
    (1007, 'James Wilson', 'jwilson@email.com', '555-0107', 'Aggressive', '2023-12-10', 'Robert Davis', 1, '2024-02-20');
GO

-- Create a view for easy portfolio summaries
CREATE OR REPLACE VIEW dbo.vw_portfolio_summary AS
SELECT
    cp.customer_id,
    cp.customer_name,
    cp.risk_tolerance,
    cp.advisor_name,
    COUNT(ch.holding_id) AS total_positions,
    COUNT(DISTINCT ch.ticker_symbol) AS unique_tickers,
    SUM(ch.shares_held * ch.cost_basis) AS total_portfolio_value,
    MIN(ch.purchase_date) AS earliest_purchase,
    MAX(ch.purchase_date) AS latest_purchase,
    cp.is_accredited_investor
FROM dbo.customer_profiles cp
LEFT JOIN dbo.customer_holdings ch ON cp.customer_id = ch.customer_id
GROUP BY
    cp.customer_id,
    cp.customer_name,
    cp.risk_tolerance,
    cp.advisor_name,
    cp.is_accredited_investor;
GO

-- Create a view showing holdings by ticker (for market exposure analysis)
CREATE OR REPLACE VIEW dbo.vw_holdings_by_ticker AS
SELECT
    ticker_symbol,
    COUNT(DISTINCT customer_id) AS num_customers,
    SUM(shares_held) AS total_shares_across_customers,
    SUM(shares_held * cost_basis) AS total_invested_value,
    AVG(cost_basis) AS avg_cost_basis,
    MIN(purchase_date) AS earliest_purchase,
    MAX(purchase_date) AS latest_purchase
FROM dbo.customer_holdings
GROUP BY ticker_symbol;
GO

-- Verify data
PRINT '===== Database Setup Complete =====';
PRINT '';

SELECT 'Customer Holdings' AS TableName, COUNT(*) AS RowCount
FROM dbo.customer_holdings
UNION ALL
SELECT 'Customer Profiles', COUNT(*)
FROM dbo.customer_profiles;
GO

PRINT '';
PRINT 'Portfolio Summary by Customer:';
SELECT * FROM dbo.vw_portfolio_summary
ORDER BY total_portfolio_value DESC;
GO

PRINT '';
PRINT 'Top Holdings by Ticker:';
SELECT TOP 10 *
FROM dbo.vw_holdings_by_ticker
ORDER BY total_invested_value DESC;
GO

PRINT '';
PRINT '===== Next Steps =====';
PRINT '1. Note your SQL Server connection details (host, database, credentials)';
PRINT '2. Store password in Databricks secrets';
PRINT '3. Create connection and foreign catalog in Databricks';
PRINT '4. Access FactSet data from Databricks Marketplace';
PRINT '5. Run the federation demo notebook: factset_federation_demo.py';
PRINT '';
PRINT 'Key Tables:';
PRINT '  - dbo.customer_holdings  (will be federated - queried in place)';
PRINT '  - dbo.customer_profiles  (customer metadata)';
PRINT '  - dbo.vw_portfolio_summary (aggregated view)';
GO

-- Optional: Grant permissions to Databricks service account
-- Uncomment and modify with your actual service account
--
-- CREATE USER [databricks_service_account] FOR LOGIN [databricks_service_account];
-- GRANT SELECT ON dbo.customer_holdings TO [databricks_service_account];
-- GRANT SELECT ON dbo.customer_profiles TO [databricks_service_account];
-- GRANT SELECT ON dbo.vw_portfolio_summary TO [databricks_service_account];
-- GRANT SELECT ON dbo.vw_holdings_by_ticker TO [databricks_service_account];
-- GO
