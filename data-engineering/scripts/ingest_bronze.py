"""
Bronze Layer Ingestion Script
Loads raw CSV files into PostgreSQL bronze schema with metadata tracking.
"""

import sys
import os
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.db_config import get_engine, test_connection


def calculate_row_hash(row):
    """Calculate MD5 hash of a row for change detection."""
    row_str = '|'.join(str(x) for x in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()


def create_bronze_schema(engine):
    """Create bronze schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.commit()
        print("✓ Bronze schema ready")


def ingest_csv_to_bronze(csv_path, table_name, engine, if_exists='replace'):
    """
    Ingest CSV file into bronze layer with additional metadata columns.
    
    Args:
        csv_path (Path): Path to CSV file
        table_name (str): Name of the bronze table (without schema prefix)
        engine: SQLAlchemy engine
        if_exists (str): How to behave if table exists ('fail', 'replace', 'append')
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"\n{'='*70}")
        print(f"Ingesting: {csv_path.name}")
        print(f"Target: bronze.{table_name}")
        print(f"{'='*70}")
        
        # Check if file exists
        if not csv_path.exists():
            print(f"✗ Error: File not found - {csv_path}")
            return False
        
        # Read CSV file
        print("Reading CSV file...")
        df = pd.read_csv(csv_path, sep="|")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        
        # Add bronze layer metadata columns
        print("Adding bronze layer metadata...")
        df['bronze_loaded_at'] = datetime.now()
        df['bronze_source_file'] = csv_path.name
        
        # Calculate row hash for each row (useful for CDC later)
        print("Calculating row hashes...")
        df['bronze_row_hash'] = df.apply(
            lambda row: calculate_row_hash(row.drop(['bronze_loaded_at', 'bronze_source_file'])),
            axis=1
        )
        
        # Load data to bronze schema
        print(f"Loading data into bronze.{table_name}...")
        df.to_sql(
            name=table_name,
            con=engine, 
            schema='bronze',
            if_exists=if_exists,
            index=False,
            chunksize=5000
        )
        
        # Verify insertion
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM bronze.{table_name}"))
            count = result.fetchone()[0]
            print(f"✓ Success! Total rows in bronze.{table_name}: {count:,}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error ingesting data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main bronze ingestion process."""
    
    print("="*70)
    print("BRONZE LAYER INGESTION")
    print("="*70)
    
    # Test database connection
    print("\nTesting database connection...")
    if not test_connection():
        print("\n✗ Database connection failed. Please check your configuration.")
        print("  Ensure Docker container is running: docker-compose up -d")
        return
    
    # Get database engine
    engine = get_engine()
    
    # Create bronze schema
    print("\nSetting up bronze schema...")
    create_bronze_schema(engine)
    
    # Define CSV files and their bronze table mappings
    data_dir = Path(__file__).parent.parent / "data" / "raw"
    
    csv_files = {
        'carrier.csv': 'bronze_carrier',
        'dme.csv': 'bronze_dme',
        'hha.csv': 'bronze_hha',
        'hospice.csv': 'bronze_hospice',
        'inpatient.csv': 'bronze_inpatient',
        'outpatient.csv': 'bronze_outpatient',
        'snf.csv': 'bronze_snf',
    }
    
    # Check if any CSV files exist
    existing_files = [f for f in csv_files.keys() if (data_dir / f).exists()]
    
    if not existing_files:
        print("\n" + "="*70)
        print("⚠ No CSV files found in data/raw/")
        print("\nPlease download the raw data files:")
        print("1. Visit: https://drive.google.com/drive/folders/1GRIfZvldvPdVoMNPjUzIYE2wHijRk11J")
        print("2. Download all CSV files")
        print(f"3. Place them in: {data_dir}")
        print("4. Run this script again")
        print("="*70)
        return
    
    # Process each file
    print(f"\nFound {len(existing_files)} CSV file(s) to process")
    success_count = 0
    
    for csv_file, table_name in csv_files.items():
        csv_path = data_dir / csv_file
        if csv_path.exists():
            if ingest_csv_to_bronze(csv_path, table_name, engine):
                success_count += 1
        else:
            print(f"\n⚠ Skipping {csv_file} - file not found")
    
    # Summary
    print("\n" + "="*70)
    print(f"INGESTION COMPLETE: {success_count}/{len(existing_files)} files loaded")
    print("="*70)
    
    # Show schema info
    if success_count > 0:
        print("\nBronze layer tables created:")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
                FROM pg_tables
                WHERE schemaname = 'bronze'
                ORDER BY tablename
            """))
            for row in result:
                print(f"  - bronze.{row[0]} ({row[1]})")


if __name__ == "__main__":
    main()
