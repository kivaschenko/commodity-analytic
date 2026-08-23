# ETL Pipeline Implementation Plan

## Overview

This document outlines the specific changes needed to complete the commodity analytics ETL pipeline. Based on the current state audit, we need to implement transformation logic, warehouse loading, and data quality checks.

## 1. Complete Transformation Phase

### 1.1 Update DataNormalizer Class

**File**: `transformation/normalizer.py`

**Changes Needed**:
- Implement currency conversion using FX rates from currency parser
- Add unit conversion logic (tons ↔ bushels for grain commodities)
- Create commodity name standardization (Ukrainian → English)
- Add price type classification logic

**New Methods to Implement**:
```python
def normalize_currency_prices(self, data: List[Dict], source: str) -> List[Dict]:
    """Convert all prices to USD using current FX rates."""

def standardize_units(self, data: List[Dict], source: str) -> List[Dict]:
    """Convert all volume/price units to standard units."""

def normalize_commodity_names(self, data: List[Dict], source: str) -> List[Dict]:
    """Map source-specific commodity names to standard names."""

def classify_price_types(self, data: List[Dict], source: str) -> List[Dict]:
    """Add price_type field (spot, futures, bid, ask, etc.)."""
```

### 1.2 Complete DataEnricher Class

**File**: `transformation/enricher.py`

**Changes Needed**:
- Complete date dimension addition
- Add market context and trading status
- Implement derived metrics calculation
- Add historical comparisons

**New Methods to Implement**:
```python
def add_market_context(self, data: List[Dict]) -> List[Dict]:
    """Add market-specific context flags."""

def calculate_derived_metrics(self, data: List[Dict]) -> List[Dict]:
    """Calculate price changes, volatility, etc."""

def add_historical_context(self, data: List[Dict]) -> List[Dict]:
    """Add YoY, MoM comparisons if historical data available."""
```

### 1.3 Update Transformation DAG

**File**: `dags/transformation_dag.py`

**Changes Needed**:
- Complete all transformation tasks for each source
- Add proper error handling and validation
- Implement data persistence to silver layer

**New Tasks to Implement**:
```python
@task()
def normalize_yfinance(normalized_data: List[Dict]) -> List[Dict]:
    """Apply normalization to YFinance data."""

@task()
def normalize_graintradecomua(cleaned_data: List[Dict], fx_rates: Dict) -> List[Dict]:
    """Apply normalization to GrainTrade UA data."""

@task()
def normalize_tripoli_land(cleaned_data: List[Dict], fx_rates: Dict) -> List[Dict]:
    """Apply normalization to Tripoli Land data."""

@task()
def enrich_all_sources(*normalized_data) -> List[Dict]:
    """Enrich all normalized data with dimensions."""

@task()
def save_silver_layer(enriched_data: List[Dict]) -> str:
    """Save transformed data to silver layer in MinIO."""
```

## 2. Implement Warehouse Loading Phase

### 2.1 Set Up Database Connection

**File**: `config/settings.py`

**Changes Needed**:
- Add database connection configuration
- Support for PostgreSQL as warehouse backend

**New Configuration**:
```python
WAREHOUSE_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", 5432)),
    "database": os.getenv("WAREHOUSE_DB", "commodity_analytics"),
    "user": os.getenv("WAREHOUSE_USER", "analytics_user"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}
```

### 2.2 Complete WarehouseLoader Class

**File**: `warehouse/loader.py`

**Changes Needed**:
- Implement actual database operations
- Add dimension key generation
- Create fact table loading logic
- Implement SCD Type 2 for dimensions

**New Methods to Implement**:
```python
def get_next_key(self, table_name: str) -> int:
    """Generate next surrogate key for dimension."""

def load_dim_date(self, data: List[Dict]) -> Dict[str, int]:
    """Load date dimension with SCD support."""

def load_dim_commodity(self, data: List[Dict]) -> Dict[str, int]:
    """Load commodity dimension with SCD support."""

def load_fact_table(self, data: List[Dict]) -> Dict[str, int]:
    """Load commodity price facts."""
```

### 2.3 Update Warehouse Load DAG

**File**: `dags/warehouse_load_dag.py`

**Changes Needed**:
- Implement all loading tasks
- Add proper transaction management
- Create error handling and rollback

**New Tasks to Implement**:
```python
@task()
def load_silver_data() -> List[Dict]:
    """Load transformed data from silver layer."""

@task()
def extract_dimensions(silver_data: List[Dict]) -> Dict[str, List[Dict]]:
    """Extract unique dimension records from fact data."""

@task()
def load_dimensions(dimensions: Dict) -> Dict[str, Dict]:
    """Load all dimension tables and return key mappings."""

@task()
def load_facts(silver_data: List[Dict], key_mappings: Dict) -> Dict[str, int]:
    """Load fact table with foreign key resolution."""
```

## 3. Implement Data Quality Checks

### 3.1 Complete Quality Checks DAG

**File**: `dags/quality_checks_dag.py`

**Changes Needed**:
- Implement actual validation queries
- Add alerting logic
- Create comprehensive checks

**New Tasks to Implement**:
```python
@task()
def check_freshness() -> Dict:
    """Check data freshness for all sources."""

@task()
def check_completeness() -> Dict:
    """Check for null values in required columns."""

@task()
def check_uniqueness() -> Dict:
    """Check for duplicate records in fact table."""

@task()
def validate_business_rules() -> Dict:
    """Check business rule compliance."""
```

### 3.2 Create Data Quality Framework

**File**: `staging/data_quality.py`

**Changes Needed**:
- Implement comprehensive validation framework
- Add statistical outlier detection
- Create data profiling capabilities

**New Classes/Methods**:
```python
class DataQualityChecker:
    def check_completeness(self, data: List[Dict]) -> Dict:
        """Check for missing values."""

    def detect_outliers(self, data: List[Dict], column: str) -> List[Dict]:
        """Statistical outlier detection."""

    def validate_schema(self, data: List[Dict], schema: Dict) -> Dict:
        """Schema validation."""
```

## 4. Update Configuration and Dependencies

### 4.1 Update Requirements

**File**: `requirements.txt`

**Add Dependencies**:
```
psycopg2-binary==2.9.7  # PostgreSQL driver
sqlalchemy==2.0.23      # ORM for database operations
pandas==2.1.4           # Data manipulation
numpy==1.26.2           # Statistical operations
scipy==1.11.4           # Advanced statistics
```

### 4.2 Update Docker Configuration

**File**: `compose.yml`

**Changes Needed**:
- Add PostgreSQL service for warehouse
- Configure database initialization
- Update environment variables

**New Service**:
```yaml
warehouse:
  image: postgres:15
  environment:
    POSTGRES_DB: commodity_analytics
    POSTGRES_USER: analytics_user
    POSTGRES_PASSWORD: ${WAREHOUSE_PASSWORD}
  volumes:
    - warehouse_data:/var/lib/postgresql/data
    - ./warehouse/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
  ports:
    - "5432:5432"
```

## 5. Create Database Schema and Initialization

### 5.1 Create Database Initialization Script

**File**: `scripts/init_warehouse.py`

**Purpose**: Initialize warehouse database with schema and initial data.

**Key Features**:
- Create all tables from schema.py
- Load initial dimension data (currencies, markets, etc.)
- Set up indexes and constraints
- Create analytical views

### 5.2 Update Schema File

**File**: `warehouse/schema.py`

**Changes Needed**:
- Complete all table DDL statements
- Add indexes for performance
- Create analytical views
- Add constraints and triggers

## 6. Testing and Validation

### 6.1 Create Test Data

**File**: `tests/test_data/`

**Create Sample Data**:
- Sample transformed data for each source
- Expected warehouse loading results
- Edge cases and error conditions

### 6.2 Implement Unit Tests

**Files**: `tests/test_*.py`

**Test Coverage**:
- Transformation functions
- Warehouse loading logic
- Data quality checks
- Error handling scenarios

### 6.3 Integration Tests

**File**: `tests/test_integration.py`

**Test Pipeline**:
- End-to-end ETL pipeline testing
- Data validation between stages
- Performance testing

## 7. Documentation Updates

### 7.1 Create Data Dictionary

**File**: `docs/DATA_DICTIONARY.md`

**Content**:
- Description of all tables and columns
- Data source mappings
- Business rules and constraints
- Data lineage documentation

### 7.2 Update Architecture Documentation

**File**: `docs/ARCHITECTURE.md`

**Content**:
- Updated pipeline architecture
- Data flow diagrams
- Component interactions
- Deployment architecture

### 7.3 Create Operations Runbook

**File**: `docs/RUNBOOK.md`

**Content**:
- Pipeline startup and shutdown procedures
- Monitoring and alerting procedures
- Backup and recovery procedures
- Troubleshooting common issues

## 8. Monitoring and Alerting

### 8.1 Implement Health Checks

**File**: `monitoring/health_checks.py`

**Features**:
- Database connectivity checks
- Pipeline status monitoring
- Data freshness validation
- Resource usage monitoring

### 8.2 Set Up Alerting

**File**: `monitoring/alerting.py`

**Features**:
- Email notifications for failures
- Slack integration for alerts
- Dashboard for pipeline health
- Escalation procedures

## Implementation Timeline

### Week 1: Foundation
- Complete DataNormalizer and DataEnricher
- Set up database connection and schema
- Implement basic warehouse loading

### Week 2: Core Pipeline
- Complete transformation DAG
- Implement warehouse loading DAG
- Add data quality checks

### Week 3: Testing and Validation
- Create comprehensive tests
- Validate end-to-end pipeline
- Performance optimization

### Week 4: Production Readiness
- Implement monitoring and alerting
- Complete documentation
- Production deployment preparation

## Risk Mitigation

### Technical Risks
- **Data Loss**: Implement transaction management and rollback
- **Performance**: Add indexing and query optimization
- **Concurrency**: Implement proper locking strategies

### Operational Risks
- **Monitoring Gaps**: Implement comprehensive logging and alerting
- **Documentation**: Create detailed runbooks and procedures
- **Testing**: Implement thorough testing at all levels

## Success Criteria

- All 4 data sources successfully transformed and loaded
- Data quality > 95% pass rate
- Pipeline runtime < 30 minutes
- Comprehensive monitoring and alerting
- Complete documentation for operations</content>
<parameter name="filePath">/home/ikost/Projects/commodity-analytic/docs/IMPLEMENTATION_PLAN.md