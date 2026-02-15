"""
IPO Analytics Data Pipeline
============================
End-to-end pipeline: Web Scraping → CSV → DuckDB → Staging → Analytics Mart

Pipeline Stages:
1. Web Scraping: Extract IPO data from sources
2. CSV Storage: Save raw data to CSV files
3. DuckDB Load: Import CSVs into DuckDB raw tables
4. Data Cleaning: Process and clean data in staging tables
5. Analytics Mart: Create final mart_ipo_analytics table
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.append(str(PROJECT_ROOT))

# Import scraper modules
from scrapers.ipo_data_scraper import IPOMetadataScraper
from scrapers.ipo_tickertape_scraper import TickertapeScraper
from scrapers.subscription_scraper import IPOSubscriptionScraper

# Configuration
CONFIG = {
    'raw_data_dir': PROJECT_ROOT / 'data' / 'raw',
    'staged_data_dir': PROJECT_ROOT / 'data' / 'staged',
    'duckdb_path': PROJECT_ROOT / 'data' / 'duckdb' / 'ipo.duckdb',
    'log_dir': PROJECT_ROOT / 'logs',
}

# Setup logging
def setup_logging():
    """Configure logging for the pipeline"""
    log_dir = CONFIG['log_dir']
    log_dir.mkdir(exist_ok=True, parents= True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'pipeline_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('IPO_Pipeline')

logger = setup_logging()


class IPODataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self):
        self.duckdb_conn = None
        self.config = CONFIG
        
    def connect_duckdb(self):
        """Establish DuckDB connection"""
        logger.info(f"Connecting to DuckDB: {self.config['duckdb_path']}")

        #Ensure duckdb directory exists
        self.config['duckdb_path'].parent.mkdir(exist_ok=True, parents=True)
        self.duckdb_conn = duckdb.connect(str(self.config['duckdb_path']))
        logger.info("DuckDB connection established")
        
    def stage_1_web_scraping(self):
        """Stage 1: Run web scraping scripts and save to CSV"""
        logger.info("=" * 70)
        logger.info("STAGE 1: WEB SCRAPING")
        logger.info("=" * 70)
        
        raw_dir = self.config['raw_data_dir']
        raw_dir.mkdir(exist_ok=True, parents=True)
        
        scrapers = [
            ('IPO Metadata', IPOMetadataScraper(), 'ipo_metadata.csv'),
            ('IPO Subscription',  IPOSubscriptionScraper(), 'ipo_subscription.csv'),
            ('Tickertape Data', TickertapeScraper(), 'tickertape_full_screener.csv'),
        ]
        
        failed_scrapers = [] 

        for scraper_name, scraper, filename in scrapers:
            try:
                logger.info(f"Running {scraper_name} scraper...")
                data = scraper.scrape()
                
                output_path = raw_dir / filename
                scraper.save_to_csv(data, output_path)
                
                logger.info(f"{scraper_name}: Saved {len(data)} records to {filename}")
            except Exception as e:
                logger.error(f"{scraper_name} failed: {str(e)}", exc_info= True)
                failed_scrapers.append(scraper_name)
            
        if failed_scrapers:
                logger.warning(f"Some scrapers failed: {','.join(failed_scrapers)}")
                logger.warning("Pipeline will continue with available data")
        else:
                logger.info("Stage 1 completed successfully\n")

    def stage_2_csv_to_raw_tables(self):
        """Stage 2: Load CSV files into DuckDB raw tables"""
        logger.info("=" * 70)
        logger.info("STAGE 2: CSV TO RAW TABLES")
        logger.info("=" * 70)
        
        raw_dir = self.config['raw_data_dir']
        
        # Define CSV to table mappings
        csv_mappings = [
            ('ipo_metadata.csv', 'raw_past_ipo_table_1'),
            ('ipo_subscription.csv', 'raw_pastipo_subscription_table_1'),
            ('tickertape_full_screener.csv', 'raw_tickertape_data_1'),
        ]
        
        loaded_tables = []

        for csv_file, table_name in csv_mappings:
            try:
                csv_path = raw_dir / csv_file
                
                if not csv_path.exists():
                    logger.warning(f"CSV file not found: {csv_file}, skipping...")
                    continue
                
                logger.info(f"Loading {csv_file} into {table_name}...")
                
                # Drop and recreate table
                self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create table from CSV
                self.duckdb_conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{csv_path}', 
                        header=true, 
                        ignore_errors=true,
                        all_varchar=false
                    )
                """)
                
                # Get row count
                row_count = self.duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                
                logger.info(f"✓ {table_name}: Loaded {row_count} records")
                loaded_tables.append(table_name)

            except Exception as e:
                logger.error(f"Failed to load {csv_file}: {str(e)}", exc_info=True)
                
        if loaded_tables:
            logger.info(f"Stage 2 completed with {len(loaded_tables)} tables loaded\n")
        else:
            raise RuntimeError("No tables were successfully loaded in Stage 2")
        
    def stage_3_data_cleaning(self):
        """Stage 3: Clean and process data into staging tables"""
        logger.info("=" * 70)
        logger.info("STAGE 3: DATA CLEANING & STAGING")
        logger.info("=" * 70)
        
        transformations_dir = PROJECT_ROOT / 'sql_transformations'
        
        # Execute transformation scripts in order
        transformation_scripts = [
            'stg_past_ipo_table.sql',
            'stg_pastipo_subscription_table.sql',
            'stg_tickertape_data_table.sql',
        ]
        
        processed_tables = []

        for script_name in transformation_scripts:
            try:
                script_path = transformations_dir / script_name
                
                if not script_path.exists():
                    logger.warning(f"Script not found: {script_name}, skipping...")
                    continue
                
                logger.info(f"Executing {script_name}...")
                
                # Read and execute SQL
                with open(script_path, 'r') as f:
                    sql_script = f.read()
                
                self.duckdb_conn.execute(sql_script)
                
                # Extract table name from script name
                table_name = script_name.replace('.sql', '')
                
                # Get row count
                row_count = self.duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                
                logger.info(f"{table_name}: Processed {row_count} records")
                processed_tables.append(table_name)
                
            except Exception as e:
                logger.error(f"Failed to execute {script_name}: {str(e)}", exc_info=True)
        
        if processed_tables:
            logger.info(f"Stage 3 completed with {len(processed_tables)} tables processed\n")
        else:
            raise RuntimeError("No staging tables were successfully created in stage 3")
    
    def stage_4_create_analytics_mart(self):
        """Stage 4: Create final analytics mart table"""
        logger.info("=" * 70)
        logger.info("STAGE 4: CREATE ANALYTICS MART")
        logger.info("=" * 70)
        
        try:
            logger.info("Creating mart_ipo_analytics table...")
            
            # Read the mart creation SQL
            mart_script_path = PROJECT_ROOT / 'sql_transformations' / 'mart_ipo_analytics.sql'

            if not mart_script_path.exists():
                raise FileNotFoundError(f"Mart script not found: {mart_script_path}")            
            
            with open(mart_script_path, 'r') as f:
                mart_sql = f.read()
            
            # Execute mart creation
            self.duckdb_conn.execute(mart_sql)
            
            # Get row count and sample data
            row_count = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM mart_ipo_analytics_1"
            ).fetchone()[0]
            
            logger.info(f"✓ mart_ipo_analytics: Created with {row_count} records")
            
            # Show sample statistics
            # stats = self.duckdb_conn.execute("""
            #     SELECT 
            #         COUNT(*) as total_records,
            #         COUNT(DISTINCT ticker) as unique_tickers,
            #         SUM(CASE WHEN has_subscription_data = 'Y' THEN 1 ELSE 0 END) as with_subscription,
            #         SUM(CASE WHEN has_market_data = 'Y' THEN 1 ELSE 0 END) as with_market_data
            #     FROM mart_ipo_analytics_1
            # """).fetchone()
            
            # logger.info(f"  Total Records: {stats[0]}")
            # logger.info(f"  Unique Tickers: {stats[1]}")
            # logger.info(f"  With Subscription Data: {stats[2]}")
            # logger.info(f"  With Market Data: {stats[3]}")
            
        except Exception as e:
            logger.error(f"Failed to create analytics mart: {str(e)}", exc_info=True)
            raise
        
        logger.info("Stage 4 completed successfully\n")
    
    def export_results(self):
        """Export final results to CSV"""
        logger.info("=" * 70)
        logger.info("EXPORTING RESULTS")
        logger.info("=" * 70)
        
        staged_dir = self.config['staged_data_dir']
        staged_dir.mkdir(exist_ok=True, parents=True)
        
        output_path = staged_dir / f"mart_ipo_analytics_{datetime.now().strftime('%Y%m%d')}.csv"
        
        try:
            self.duckdb_conn.execute(f"""
                COPY mart_ipo_analytics_1 
                TO '{output_path}' 
                (HEADER, DELIMITER ',')
            """)
            
            logger.info(f"Results exported to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to export results: {str(e)}", exc_info=True)
            raise
    
    def run(self):
        """Execute complete pipeline"""
        start_time = datetime.now()
        
        logger.info("╔" + "═" * 68 + "╗")
        logger.info("║" + " " * 20 + "IPO ANALYTICS PIPELINE" + " " * 26 + "║")
        logger.info("╚" + "═" * 68 + "╝")
        logger.info(f"Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        try:
            # Connect to DuckDB
            self.connect_duckdb()
            
            # Run pipeline stages
            self.stage_1_web_scraping()
            self.stage_2_csv_to_raw_tables()
            self.stage_3_data_cleaning()
            self.stage_4_create_analytics_mart()
            self.export_results()
            
            # Calculate duration
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("╔" + "═" * 68 + "╗")
            logger.info("║" + " " * 23 + "PIPELINE COMPLETED" + " " * 27 + "║")
            logger.info("╚" + "═" * 68 + "╝")
            logger.info(f"Duration: {duration}")
            logger.info(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            logger.error("╔" + "═" * 68 + "╗")
            logger.error("║" + " " * 25 + "PIPELINE FAILED" + " " * 28 + "║")
            logger.error("╚" + "═" * 68 + "╝")
            logger.error(f"Error: {str(e)}")
            raise
        
        finally:
            if self.duckdb_conn:
                self.duckdb_conn.close()
                logger.info("DuckDB connection closed")


if __name__ == "__main__":
    pipeline = IPODataPipeline()
    pipeline.run()