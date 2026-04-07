"""
Documentation Files
"""

# ARCHITECTURE.md - System Architecture
ARCHITECTURE_CONTENT = """
# Data Pipeline Architecture

## System Overview

```
Data Sources
    ↓
[Extraction Layer] → Bronze (Raw Data in S3/MinIO)
    ↓
[Staging Layer] → Data Quality Checks
    ↓
[Transformation Layer] → Silver (Cleaned Data)
    ↓
[Warehouse Layer] → Gold (Star Schema OLAP)
    ↓
[Analytics Layer] → ML Features & Views
    ↓
[Monitoring Layer] → Health Checks & Alerts
```

## Data Sources

- **Yahoo Finance**: Global commodity prices via yfinance API
- **Investing.com**: Global financial market data via scraping
- **GrainTrade.com.ua**: Ukrainian grain market prices
- **APK Inform**: Ukrainian agricultural information
- **Currency Rates**: Exchange rate data

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Apache Airflow 3.0+ | DAG scheduling and monitoring |
| Extraction | Python + APIs | Data collection from multiple sources |
| Processing | PySpark + Pandas | Large-scale data transformation |
| Storage | S3/MinIO | Data lake (bronze/silver layers) |
| Warehouse | Snowflake/PostgreSQL | OLAP database (gold layer) |
| Analytics | SQL Views | Business intelligence views |
| Monitoring | Python + Slack | Health checks and alerting |

## Data Flow

### Daily Pipeline

1. **extraction_dag** (0:00 UTC) - Extracts data from all sources
2. **transformation_dag** - Cleans, normalizes, and enriches data
3. **warehouse_load_dag** - Loads into warehouse fact/dimension tables
4. **quality_checks_dag** - Validates warehouse data
5. **backfill_dag** - Manual trigger for historical backfills

### Data Medallion Architecture

- **Bronze**: Raw data from sources (staging/)
- **Silver**: Cleaned, normalized data (transformation/)
- **Gold**: Dimensional model in warehouse (warehouse/)

## Schema Design

### Fact Table
- commodity_prices_fact
  - Measures: open, high, low, close, volume
  - Dimensions: date, commodity, market, source, currency
  - Partition: date_key

### Dimension Tables
- dim_date: Calendar dimensions
- dim_commodity: Commodity master data
- dim_market: Market/exchange master data
- dim_source: Data source master data
- dim_currency: Currency codes and metadata

### Aggregate Tables
- daily_price_summary: OHLCV by day, commodity, market
- weekly_commodity_summary: Weekly averages
- monthly_commodity_summary: Monthly summaries with volatility

## Security & Access Control

- Environment-specific settings (dev/staging/prod)
- Encrypted API keys and credentials
- Database connection pooling
- Role-based access control (RBAC) for warehouse

## Monitoring & Alerting

- DAG execution monitoring in Airflow UI
- Data quality metric dashboards
- Slack/email alerts for failures
- Health check reports
- Performance SLA tracking

## Performance Optimization

- Partitioning fact table by date_key
- Indexes on common query dimensions
- Materialized aggregate views
- Incremental data loading
- Compression for storage efficiency
"""

# DATA_DICTIONARY.md - Data Dictionary
DATA_DICTIONARY_CONTENT = """
# Data Dictionary

## Fact Table: commodity_prices_fact

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| price_id | BIGINT | Unique fact record ID | Primary Key |
| date_key | INT | Reference to dim_date | FK, partition key |
| commodity_key | INT | Reference to dim_commodity | FK |
| market_key | INT | Reference to dim_market | FK |
| source_key | INT | Reference to dim_source | FK |
| currency_key | INT | Reference to dim_currency | FK |
| open_price | DECIMAL | Opening price | Per unit, normalized |
| high_price | DECIMAL | Highest price of day | Per unit |
| low_price | DECIMAL | Lowest price of day | Per unit |
| close_price | DECIMAL | Closing price | Per unit |
| volume | DECIMAL | Trading volume | In base unit (tons) |
| created_at | TIMESTAMP | Record creation time | UTC |
| updated_at | TIMESTAMP | Last update time | UTC |

## Dimension Table: dim_commodity

| Column | Type | Description |
|--------|------|-------------|
| commodity_key | INT | Primary Key |
| commodity_name | VARCHAR | Standard commodity name |
| commodity_type | VARCHAR | Type: grain, oil_crop, etc. |
| category | VARCHAR | Category classification |
| unit | VARCHAR | Measurement unit (ton, bushel, etc.) |
| grade | VARCHAR | Commodity grade/quality |
| origin_country | VARCHAR | Country of origin |
| is_active | BOOLEAN | Currently traded? |
| created_at | TIMESTAMP | SCD Type 2 |
| ended_at | TIMESTAMP | SCD Type 2 |

## Dimension Table: dim_date

| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | YYYYMMDD format |
| calendar_date | DATE | Actual calendar date |
| year | INT | Calendar year |
| quarter | INT | Quarter (1-4) |
| month | INT | Month (1-12) |
| day | INT | Day of month (1-31) |
| week_of_year | INT | ISO week number |
| day_of_week | VARCHAR | Monday, Tuesday, etc. |
| is_weekend | BOOLEAN | Saturday or Sunday? |
| is_holiday | BOOLEAN | Major trading holiday? |
| trading_status | VARCHAR | active, halted |

## Dimension Table: dim_market

| Column | Type | Description |
|--------|------|-------------|
| market_key | INT | Primary Key |
| market_name | VARCHAR | Market name (Chicago, Kyiv, etc.) |
| exchange | VARCHAR | Exchange name (CBOT, ICEECX, etc.) |
| country | VARCHAR | Country of market |
| timezone | VARCHAR | Market timezone |
| trading_hours | VARCHAR | Market trading hours |
| is_active | BOOLEAN | Currently active? |
| created_at | TIMESTAMP | SCD Type 2 |
| ended_at | TIMESTAMP | SCD Type 2 |

## Dimension Table: dim_source

| Column | Type | Description |
|--------|------|-------------|
| source_key | INT | Primary Key |
| source_name | VARCHAR | Source identifier |
| parser_type | VARCHAR | Parser implementation |
| data_type | VARCHAR | Data type (prices, volumes, etc.) |
| reliability_rating | INT | 1-5 rating |
| update_frequency | VARCHAR | daily, hourly, etc. |
| api_endpoint | VARCHAR | API URL if applicable |
| is_active | BOOLEAN | Currently extracting? |

## Dimension Table: dim_currency

| Column | Type | Description |
|--------|------|-------------|
| currency_key | INT | Primary Key |
| currency_code | VARCHAR | ISO 4217 code (USD, EUR, UAH) |
| currency_name | VARCHAR | Full name |
| country | VARCHAR | Country of currency |
"""

# RUNBOOK.md - Operations Runbook
RUNBOOK_CONTENT = """
# Operations Runbook

## Daily Operations

### Normal Daily Workflow
1. Monitor Airflow UI for DAG runs
2. Check Slack for alerts
3. Review data quality reports
4. Monitor warehouse performance

### Monitoring Dashboards
- Airflow UI: http://localhost:8080
- Warehouse Query Analytics: [snowflake UI]
- Data Quality Dashboard: [grafana/tableau]

## Troubleshooting

### Extraction DAG Failed
1. Check source availability
2. Review extraction logs in `dags/logs/`
3. Verify API credentials
4. Retry failed tasks in Airflow UI

### Data Quality Check Failure
1. Review quality check details
2. Identify root cause (duplicate, null, anomaly)
3. Decide: retry, investigate, or skip
4. Update quality rules if needed

### Warehouse Connection Issues
1. Verify database connectivity
2. Check credentials in config
3. Verify network/firewall rules
4. Test connection manually

### Performance Issues
1. Check query execution plans
2. Review warehouse compute usage
3. Analyze table statistics
4. Consider re-partitioning

## Maintenance Tasks

### Weekly
- Review data quality metrics
- Check storage growth
- Verify backup completion
- Review alert trends

### Monthly
- Analyze pipeline performance
- Update statistics/indexes
- Archive old logs
- Review cost optimization

### Quarterly
- Capacity planning
- Schema review
- Disaster recovery testing
- Technology updates

## Backfill Procedures

### Backfill Example: Fill Gap from 2025-01-01 to 2025-01-31
```
1. Trigger backfill_dag with:
   - start_date: 2025-01-01
   - end_date: 2025-01-31
    - sources: [yfinance, graintradecomua, apkinform]

2. Monitor execution

3. Verify data:
   SELECT COUNT(*) FROM commodity_prices_fact 
   WHERE date_key BETWEEN 20250101 AND 20250131

4. Refresh aggregates

5. Validate with quality checks
```

## Disaster Recovery

### Data Loss Recovery
1. Restore from S3 backup
2. Reload into warehouse
3. Refresh aggregates
4. Run full quality checks

### Schema Recovery
1. Restore schema from git
2. Recreate tables/indexes
3. Backfill data
4. Verify integrity

## Contact & Escalation

- Data Engineering Team: data-eng@company.com
- On-Call: [on-call schedule]
- Slack Channel: #data-pipeline
"""

# TROUBLESHOOTING.md - Troubleshooting Guide
TROUBLESHOOTING_CONTENT = """
# Troubleshooting Guide

## Common Issues

### Issue: DAG Not Triggering
**Symptoms**: DAG does not run at scheduled time

**Causes**:
- Scheduler not running
- DAG paused in Airflow
- Cron expression incorrect
- Dependencies not met

**Solution**:
1. Check scheduler status: `airflow scheduler`
2. Verify DAG not paused in UI
3. Verify schedule expression
4. Check upstream DAG dependencies

### Issue: High Null Values in Data
**Symptoms**: Data quality check flags null values

**Causes**:
- Source API temporarily down
- Parser not handling edge cases
- Network connectivity issue
- Schema mismatch

**Solution**:
1. Verify source availability
2. Check parser logs
3. Run manual extraction test
4. Review source data format

### Issue: Warehouse Queries Slow
**Symptoms**: Analytics queries timeout

**Causes**:
- Missing indexes
- Outdated statistics
- Warehouse compute insufficient
- Complex query joins

**Solution**:
1. Analyze query plan
2. Run `ANALYZE` on tables
3. Add missing indexes
4. Scale warehouse compute
5. Optimize SQL

### Issue: Duplicate Records in Warehouse
**Symptoms**: Duplicate records in fact table

**Causes**:
- Multiple extraction runs
- Upsert logic failure
- Race condition in loading
- Data quality check missed

**Solution**:
1. Identify duplicates: 
   ```sql
   SELECT date_key, commodity_key, market_key, COUNT(*) 
   FROM commodity_prices_fact 
   GROUP BY date_key, commodity_key, market_key 
   HAVING COUNT(*) > 1
   ```
2. Delete duplicates (keep latest)
3. Verify upsert logic
4. Implement duplicate check in quality

### Issue: Memory Issues During Transformation
**Symptoms**: PySpark job fails with OOM

**Causes**:
- Large dataset size
- Insufficient executor memory
- Memory leak in transformation
- No partitioning

**Solution**:
1. Increase executor memory in DAG
2. Implement batch processing
3. Add data partitioning
4. Profile memory usage

## Debugging Techniques

### Enable Debug Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Manual Data Extract Test
```python
from parser_services.yfinance_parser import YFinanceParser
parser = YFinanceParser()
data = parser.parse()
print(len(data))
print(data[0])
```

### Warehouse Query Test
```sql
SELECT COUNT(*) as row_count FROM commodity_prices_fact;
SELECT DISTINCT commodity_key FROM commodity_prices_fact LIMIT 10;
```

### View Logs
```bash
# Airflow logs
tail -f ~/airflow/logs/extraction_dag/extract_yfinance/*.log

# Application logs
tail -f logs/pipeline.log

# Warehouse logs
SELECT * FROM warehouse_logs ORDER BY timestamp DESC LIMIT 100;
```

## Performance Tuning

### Optimize Extraction
- Use incremental extracts (last 24h only)
- Batch API calls
- Implement retry with backoff
- Cache stable master data

### Optimize Transformation
- Use Spark partitioning
- Reduce data shuffles
- Cache intermediate dataframes
- Parallelize operations

### Optimize Warehouse
- Partition fact table by date
- Create indexes on foreign keys
- Use columnar compression
- Refresh statistics regularly
- Archive historical data

## Health Check Checklist

Daily:
- [ ] All DAGs completed successfully
- [ ] No quality check failures
- [ ] No Slack alerts
- [ ] Warehouse connectivity OK

Weekly:
- [ ] Storage growth within limits
- [ ] Query performance acceptable
- [ ] Backup completed
- [ ] Alert trends stable

Monthly:
- [ ] Data freshness verified
- [ ] Cost within budget
- [ ] Disaster recovery tested
- [ ] Logs rotated
"""

def create_documentation_files() -> Dict[str, str]:
    """Create all documentation files."""
    return {
        "ARCHITECTURE.md": ARCHITECTURE_CONTENT,
        "DATA_DICTIONARY.md": DATA_DICTIONARY_CONTENT,
        "RUNBOOK.md": RUNBOOK_CONTENT,
        "TROUBLESHOOTING.md": TROUBLESHOOTING_CONTENT,
    }
