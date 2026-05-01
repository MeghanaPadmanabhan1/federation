# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample `equity_holdings` Data
# MAGIC
# MAGIC This notebook produces a CSV file you can load into your on-premise
# MAGIC Microsoft SQL Server to replicate the federation source for this demo.
# MAGIC The schema and column names match what the rest of the project expects.
# MAGIC
# MAGIC ## What it generates
# MAGIC
# MAGIC - `~/equity_holdings.csv` — the rows to load
# MAGIC - SQL Server DDL printed at the bottom — copy/paste into your SQL Server
# MAGIC   client to create the target table before loading the CSV
# MAGIC
# MAGIC ## How the reader uses it
# MAGIC
# MAGIC 1. Run this notebook (Databricks or local Python — no SDK required).
# MAGIC 2. Create the `dbo.equity_holdings` table in your on-prem SQL Server using
# MAGIC    the printed DDL.
# MAGIC 3. Bulk-load the CSV into that table (use whatever your SQL Server tooling
# MAGIC    supports: `bcp`, `BULK INSERT`, the SSMS Import Wizard, etc.).
# MAGIC 4. Continue with the federation setup in `README.md` and `SETUP_GUIDE.md`.
# MAGIC
# MAGIC No connection is made to SQL Server from this notebook — the CSV is the
# MAGIC handoff.

# COMMAND ----------

import csv
import os
import random
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Adjust `NUM_HOLDINGS` and `MAX_SHARES` if you want a different portfolio
# MAGIC size. The output path is portable across Databricks and local Python.

# COMMAND ----------

NUM_HOLDINGS = 200          # how many distinct equity positions to generate
MAX_SHARES = 10_000         # upper bound on number_of_shares per holding
OUTPUT_PATH = Path(os.path.expanduser("~/equity_holdings.csv"))
RANDOM_SEED = 42            # deterministic output across runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ticker universe
# MAGIC
# MAGIC A curated list of well-known US-listed tickers across sectors. These are
# MAGIC chosen to have high coverage in FactSet's symbology + fundamentals tables
# MAGIC so the downstream join produces meaningful results.

# COMMAND ----------

TICKERS = [
    # Tech & Communication Services
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL",
    "ADBE", "CRM", "AMD", "CSCO", "INTC", "IBM", "QCOM", "TXN", "INTU", "MU",
    "ACN", "NOW", "AMAT", "LRCX", "KLAC", "ADI", "MRVL", "SNPS", "CDNS", "FTNT",
    "PANW", "CRWD", "ANET", "SHOP", "UBER", "ABNB", "NFLX", "T", "VZ", "TMUS",
    "CMCSA", "CHTR", "DIS", "EA", "MTCH", "ROKU", "PINS", "SNAP", "PYPL", "SQ",
    # Financials
    "JPM", "BAC", "WFC", "C", "GS", "MS", "USB", "PNC", "TFC", "COF",
    "AXP", "BLK", "SCHW", "CB", "AIG", "MET", "PGR", "MMC", "ICE", "CME",
    "SPGI", "MCO", "TRV", "ALL", "AFL", "PRU", "HIG", "STT", "BK", "AMP",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT", "AMGN", "DHR",
    "BMY", "GILD", "CVS", "MDT", "SYK", "ISRG", "REGN", "VRTX", "ZTS", "BSX",
    "BDX", "EW", "IDXX", "IQV", "MTD", "RMD", "HUM", "CI", "ELV", "MCK",
    # Consumer Discretionary & Staples
    "WMT", "PG", "KO", "PEP", "COST", "HD", "MCD", "NKE", "SBUX", "LOW",
    "TGT", "BKNG", "MO", "PM", "MDLZ", "EL", "CL", "GIS", "KMB", "KR",
    "ROST", "TJX", "DG", "DLTR", "AZO", "ORLY", "YUM", "CMG", "F", "GM",
    # Industrials
    "BA", "CAT", "HON", "GE", "RTX", "LMT", "NOC", "GD", "DE", "UPS",
    "FDX", "UNP", "CSX", "NSC", "MMM", "ETN", "ITW", "EMR", "PH", "ROK",
    "FAST", "GWW", "JCI", "PCAR", "LUV", "DAL", "UAL", "AAL",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "DVN",
    "FANG", "KMI", "WMB", "OKE", "HAL", "BKR", "HES",
    # Utilities
    "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "D", "XEL", "AEE", "AWK",
    "ED", "PEG", "EIX", "FE", "ETR", "ES", "WEC", "AES", "PPL", "CMS",
    # Real Estate
    "AMT", "PLD", "EQIX", "CCI", "PSA", "WELL", "O", "SBAC", "EQR", "AVB",
    "MAA", "ESS", "UDR", "CPT", "BXP", "KIM", "REG", "FRT",
    # Materials
    "LIN", "APD", "ECL", "SHW", "FCX", "NEM", "DD", "DOW", "NUE", "STLD",
    "MLM", "VMC", "IFF", "CTVA", "BALL", "AMCR", "AVY",
]

# Trim/extend to NUM_HOLDINGS without duplicating tickers (keeps schema clean).
assert len(TICKERS) >= NUM_HOLDINGS, (
    f"Ticker universe ({len(TICKERS)}) < NUM_HOLDINGS ({NUM_HOLDINGS}). "
    "Lower NUM_HOLDINGS or extend TICKERS."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate rows

# COMMAND ----------

random.seed(RANDOM_SEED)
sampled = random.sample(TICKERS, NUM_HOLDINGS)

rows = [
    {
        "symbol": ticker,
        "instrument_type": "Equity",
        "number_of_shares": random.randint(1, MAX_SHARES),
    }
    for ticker in sampled
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write CSV

# COMMAND ----------

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with OUTPUT_PATH.open("w", newline="") as f:
    writer = csv.DictWriter(
        f, fieldnames=["symbol", "instrument_type", "number_of_shares"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows -> {OUTPUT_PATH}")
print()
print("First five rows:")
for r in rows[:5]:
    print(" ", r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Server DDL
# MAGIC
# MAGIC Run this in your on-prem SQL Server (any client — SSMS, Azure Data Studio,
# MAGIC `sqlcmd`) **before** loading the CSV. The column names and types must
# MAGIC match exactly so the federated catalog and downstream views work without
# MAGIC modification.

# COMMAND ----------

DDL = """\
-- ===== Run on your on-prem Microsoft SQL Server =====
IF OBJECT_ID('dbo.equity_holdings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.equity_holdings (
        symbol            NVARCHAR(16)  NOT NULL,
        instrument_type   NVARCHAR(32)  NOT NULL,
        number_of_shares  INT           NOT NULL
    );
END;
GO

-- Load equity_holdings.csv into the table using whatever SQL Server tooling
-- you prefer (BULK INSERT, bcp, SSMS Import Wizard, Azure Data Studio import,
-- Python pyodbc + pandas, etc.). The CSV has a header row and three columns
-- in the order matching the table above.
"""

print(DDL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. Copy `equity_holdings.csv` to a host that can reach your SQL Server.
# MAGIC 2. Run the DDL above in SQL Server.
# MAGIC 3. Load the CSV into `dbo.equity_holdings`.
# MAGIC 4. Follow `SETUP_GUIDE.md` to create the federation connection and
# MAGIC    foreign catalog (`mp_portfolio_federated`) in Databricks.
# MAGIC 5. Run the `Portfolio Federation Pipeline` workflow (or the
# MAGIC    `factset_federation_demo` notebook) to materialise the views and
# MAGIC    refresh the dashboard.
