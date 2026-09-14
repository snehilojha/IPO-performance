"""
IPO Analytics Data Pipeline
============================
End-to-end pipeline: Web Scraping -> CSV -> DuckDB -> Staging -> Matching -> Analytics Mart -> Checks

Pipeline Stages:
1. Web Scraping: Extract IPO data from sources into data/raw/<YYYY-MM-DD>/
2. Raw Load: Import raw CSVs and seed files into DuckDB
3. Staging: Clean each source into stg_* tables
4. Matching + Mart: Link sources (int_*), build mart_ipo_analytics and the join funnel
5. Data-Quality Checks: Every query in sql_checks/ must return no rows
6. Export: Mart as Parquet + CSV, join funnel as CSV

The DuckDB file is a build artifact and is recreated on every run.

Usage:
    python pipeline.py                                    # scrape today's snapshot, then build
    python pipeline.py --snapshot 2026-02-15 --skip-scrape  # rebuild from a saved snapshot
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.append(str(PROJECT_ROOT))

# Configuration
CONFIG = {
    'raw_data_dir': PROJECT_ROOT / 'data' / 'raw',
    'seeds_dir': PROJECT_ROOT / 'data' / 'seeds',
    'mart_dir': PROJECT_ROOT / 'data' / 'mart',
    'reports_dir': PROJECT_ROOT / 'reports',
    'duckdb_path': PROJECT_ROOT / 'data' / 'duckdb' / 'ipo.duckdb',
    'log_dir': PROJECT_ROOT / 'logs',
    'transformations_dir': PROJECT_ROOT / 'sql_transformations',
    'checks_dir': PROJECT_ROOT / 'sql_checks',
}

# Thresholds used by the matching models and the data-quality checks
THRESHOLDS = {
    'subscription_match_min_score': 0.90,
    'market_match_min_score': 0.90,
    'min_mart_rows': 1000,
    'min_subscription_coverage': 0.90,
    'min_market_coverage': 0.90,
}

# Raw CSV -> DuckDB table. Columns listed here are added as NULL when a scrape lacks them
# (e.g. a subscription scrape with no SME rows has no "Company Name" column).
RAW_TABLES = [
    ('ipo_metadata.csv', 'raw_past_ipo', [
        'symbol', 'company', 'ipostartdate', 'ipoenddate', 'pricerange',
        'listingdate', 'securitytype', 'issueprice',
    ]),
    ('ipo_subscription.csv', 'raw_ipo_subscription', [
        '~id', 'Company', 'Company Name', 'Closing Date', 'Close Date', 'Open Date',
        '~Issue_Open_Date', '~Issue_Close_Date',
        'Total Issue Amount (Incl.Firm reservations) (Rs.cr.)',
        'QIB (x)', 'NII (x)', 'sNII (x)', 'bNII (x)', 'Retail (x)', 'Employee (x)', 'Total (x)',
        'Applications', 'ipo_type', 'year',
    ]),
    ('tickertape_full_screener.csv', 'raw_tickertape', [
        'sid', 'ticker', 'name', 'sector', 'subindustry', 'market_cap', 'last_price',
        'pe', 'roe', 'pb', '4w_return_pct', '1d_return_pct',
    ]),
]

# Seed CSV -> DuckDB table with typed columns
SEED_TABLES = [
    ('excluded_issues.csv', 'seed_excluded_issues',
     {'ticker': 'VARCHAR', 'ipo_end_dt': 'DATE', 'reason': 'VARCHAR', 'note': 'VARCHAR'}),
    ('manual_subscription_matches.csv', 'seed_manual_subscription_matches',
     {'ticker': 'VARCHAR', 'ipo_end_dt': 'DATE', 'subscription_company_name': 'VARCHAR',
      'subscription_ipo_end_dt': 'DATE', 'note': 'VARCHAR'}),
    ('manual_ticker_matches.csv', 'seed_manual_ticker_matches',
     {'ticker': 'VARCHAR', 'tickertape_sid': 'VARCHAR', 'decision': 'VARCHAR', 'note': 'VARCHAR'}),
]

STAGING_MODELS = [
    ('00_macros.sql', None),
    ('stg_past_ipo.sql', 'stg_past_ipo'),
    ('stg_ipo_subscription.sql', 'stg_ipo_subscription'),
    ('stg_tickertape.sql', 'stg_tickertape'),
]

MART_MODELS = [
    ('int_ipo_subscription_match.sql', 'int_ipo_subscription_match'),
    ('int_ipo_market_match.sql', 'int_ipo_market_match'),
    ('mart_ipo_analytics.sql', 'mart_ipo_analytics'),
    ('audit_join_funnel.sql', 'audit_join_funnel'),
]

logger = logging.getLogger('IPO_Pipeline')


class DataQualityError(RuntimeError):
    """Raised when one or more sql_checks return violating rows"""


# Setup logging
def setup_logging():
    """Configure logging for the pipeline"""
    log_dir = CONFIG['log_dir']
    log_dir.mkdir(exist_ok=True, parents=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'pipeline_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


class IPODataPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, snapshot: date, skip_scrape: bool = False, paths: dict | None = None,
                 thresholds: dict | None = None):
        self.duckdb_conn = None
        self.config = {**CONFIG, **(paths or {})}
        self.thresholds = {**THRESHOLDS, **(thresholds or {})}
        self.snapshot = snapshot
        self.skip_scrape = skip_scrape
        self.raw_dir = self.config['raw_data_dir'] / snapshot.isoformat()

    def validate_snapshot(self):
        """Refuse to overwrite an existing snapshot or build from a missing one"""
        if self.skip_scrape:
            if not self.raw_dir.is_dir():
                raw_root = self.config['raw_data_dir']
                available = sorted(p.name for p in raw_root.iterdir() if p.is_dir()) if raw_root.is_dir() else []
                raise FileNotFoundError(
                    f"No raw snapshot at {self.raw_dir}. Available snapshots: {', '.join(available) or 'none'}"
                )
            return

        # Scrapers always fetch today's data, so a past date would mislabel it
        if self.snapshot != date.today():
            raise ValueError(
                f"Cannot scrape into snapshot {self.snapshot}: scraping captures today's data. "
                "Use --skip-scrape to rebuild from an existing snapshot."
            )
        if self.raw_dir.is_dir() and any(self.raw_dir.glob('*.csv')):
            raise FileExistsError(
                f"Snapshot {self.raw_dir} already exists and will not be overwritten. "
                "Use --skip-scrape to rebuild from it."
            )

    def connect_duckdb(self):
        """Create a fresh DuckDB database so no stale tables survive between runs"""
        db_path = self.config['duckdb_path']
        logger.info(f"Connecting to DuckDB: {db_path}")

        db_path.parent.mkdir(exist_ok=True, parents=True)
        for stale in (db_path, db_path.with_name(db_path.name + '.wal')):
            stale.unlink(missing_ok=True)
        self.duckdb_conn = duckdb.connect(str(db_path))

        # Parameters read by the SQL models via getvariable()
        self.duckdb_conn.execute(f"SET VARIABLE snapshot_date = DATE '{self.snapshot.isoformat()}'")
        for name, value in self.thresholds.items():
            self.duckdb_conn.execute(f"SET VARIABLE {name} = {value}")
        logger.info("DuckDB connection established")

    def stage_1_web_scraping(self):
        """Stage 1: Run web scraping scripts and save to CSV"""
        logger.info("=" * 70)
        logger.info("STAGE 1: WEB SCRAPING")
        logger.info("=" * 70)

        if self.skip_scrape:
            logger.info(f"Skipped (--skip-scrape): using existing snapshot {self.raw_dir}\n")
            return

        # Imported here so rebuilds and tests don't need the scraper dependencies
        from scrapers.ipo_data_scraper import IPOMetadataScraper
        from scrapers.ipo_tickertape_scraper import TickertapeScraper
        from scrapers.subscription_scraper import IPOSubscriptionScraper

        raw_dir = self.raw_dir
        raw_dir.mkdir(exist_ok=True, parents=True)

        scrapers = [
            ('IPO Metadata', IPOMetadataScraper(), 'ipo_metadata.csv'),
            ('IPO Subscription', IPOSubscriptionScraper(), 'ipo_subscription.csv'),
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
                logger.error(f"{scraper_name} failed: {str(e)}", exc_info=True)
                failed_scrapers.append(scraper_name)

        if failed_scrapers:
            raise RuntimeError(f"Scrapers failed: {', '.join(failed_scrapers)}")
        logger.info("Stage 1 completed successfully\n")

    def stage_2_load_raw_tables(self):
        """Stage 2: Load raw CSV files and seed files into DuckDB"""
        logger.info("=" * 70)
        logger.info("STAGE 2: LOAD RAW TABLES AND SEEDS")
        logger.info("=" * 70)

        for csv_file, table_name, expected_columns in RAW_TABLES:
            csv_path = self.raw_dir / csv_file
            if not csv_path.exists():
                raise FileNotFoundError(f"Raw file missing from snapshot: {csv_path}")

            # All text: types are cast explicitly in staging, so they don't depend on what the sniffer sees
            logger.info(f"Loading {csv_file} into {table_name}...")
            self.duckdb_conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv('{csv_path.as_posix()}', header=true, all_varchar=true)
            """)

            existing = {row[0] for row in self.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()}
            for column in expected_columns:
                if column not in existing:
                    logger.warning(f"{table_name}: column '{column}' missing from raw file, added as NULL")
                    self.duckdb_conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{column}" VARCHAR')

            self.log_row_count(table_name)

        for csv_file, table_name, columns in SEED_TABLES:
            seed_path = self.config['seeds_dir'] / csv_file
            if not seed_path.exists():
                raise FileNotFoundError(f"Seed file missing: {seed_path}")

            column_spec = ', '.join(f"'{name}': '{dtype}'" for name, dtype in columns.items())
            self.duckdb_conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv('{seed_path.as_posix()}', header=true, auto_detect=false,
                                       columns={{{column_spec}}})
            """)
            self.log_row_count(table_name)

        logger.info("Stage 2 completed successfully\n")

    def stage_3_staging(self):
        """Stage 3: Clean each source into staging tables"""
        logger.info("=" * 70)
        logger.info("STAGE 3: STAGING")
        logger.info("=" * 70)

        self.run_models(STAGING_MODELS)
        logger.info("Stage 3 completed successfully\n")

    def stage_4_matching_and_mart(self):
        """Stage 4: Match sources, build the analytics mart and the join funnel"""
        logger.info("=" * 70)
        logger.info("STAGE 4: MATCHING AND ANALYTICS MART")
        logger.info("=" * 70)

        self.run_models(MART_MODELS)

        funnel = self.duckdb_conn.execute(
            "SELECT step, stage, segment, n FROM audit_join_funnel ORDER BY step, segment"
        ).fetchall()
        logger.info("Join funnel (step, stage, segment, rows):")
        for step, stage, segment, n in funnel:
            logger.info(f"  {step}. {stage:<24} {segment:<10} {n:>6}")

        logger.info("Stage 4 completed successfully\n")

    def stage_5_data_quality_checks(self):
        """Stage 5: Run every sql_checks/*.sql; any returned row fails the pipeline"""
        logger.info("=" * 70)
        logger.info("STAGE 5: DATA-QUALITY CHECKS")
        logger.info("=" * 70)

        check_paths = sorted(self.config['checks_dir'].glob('*.sql'))
        if not check_paths:
            raise FileNotFoundError(f"No checks found in {self.config['checks_dir']}")

        failed = []
        for check_path in check_paths:
            result = self.duckdb_conn.execute(check_path.read_text(encoding='utf-8'))
            columns = [col[0] for col in result.description]
            rows = result.fetchall()

            if rows:
                failed.append(check_path.stem)
                logger.error(f"[FAIL] {check_path.stem}: {len(rows)} violating row(s)")
                for row in rows[:5]:
                    logger.error(f"    {dict(zip(columns, row))}")
            else:
                logger.info(f"[PASS] {check_path.stem}")

        if failed:
            raise DataQualityError(f"{len(failed)} data-quality check(s) failed: {', '.join(failed)}")
        logger.info(f"Stage 5 completed: all {len(check_paths)} checks passed\n")

    def export_results(self):
        """Export the mart (Parquet + CSV) and the join funnel (CSV)"""
        logger.info("=" * 70)
        logger.info("EXPORTING RESULTS")
        logger.info("=" * 70)

        mart_dir = self.config['mart_dir']
        reports_dir = self.config['reports_dir']
        mart_dir.mkdir(exist_ok=True, parents=True)
        reports_dir.mkdir(exist_ok=True, parents=True)

        suffix = self.snapshot.isoformat()
        exports = [
            ('mart_ipo_analytics', mart_dir / f'mart_ipo_analytics_{suffix}.parquet', "FORMAT PARQUET"),
            ('mart_ipo_analytics', mart_dir / f'mart_ipo_analytics_{suffix}.csv', "HEADER, DELIMITER ','"),
            ('audit_join_funnel', reports_dir / f'join_funnel_{suffix}.csv', "HEADER, DELIMITER ','"),
        ]
        for table_name, output_path, options in exports:
            self.duckdb_conn.execute(f"COPY {table_name} TO '{output_path.as_posix()}' ({options})")
            logger.info(f"Exported {table_name} to: {output_path}")

    def run_models(self, models):
        """Execute SQL model files in order and log the row count of each table they build"""
        for script_name, table_name in models:
            script_path = self.config['transformations_dir'] / script_name
            logger.info(f"Executing {script_name}...")
            self.duckdb_conn.execute(script_path.read_text(encoding='utf-8'))
            if table_name:
                self.log_row_count(table_name)

    def log_row_count(self, table_name):
        row_count = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"[OK] {table_name}: {row_count} rows")

    def build(self):
        """Stages 2-5 plus export, against the current snapshot's raw files"""
        self.connect_duckdb()
        self.stage_2_load_raw_tables()
        self.stage_3_staging()
        self.stage_4_matching_and_mart()
        self.stage_5_data_quality_checks()
        self.export_results()

    def run(self):
        """Execute complete pipeline"""
        start_time = datetime.now()

        logger.info("+" + "-" * 68 + "+")
        logger.info("|" + " " * 20 + "IPO ANALYTICS PIPELINE" + " " * 26 + "|")
        logger.info("+" + "-" * 68 + "+")
        logger.info(f"Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Snapshot: {self.snapshot} ({'rebuild, no scraping' if self.skip_scrape else 'scrape + build'})\n")

        try:
            self.validate_snapshot()
            self.stage_1_web_scraping()
            self.build()

            # Calculate duration
            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("+" + "-" * 68 + "+")
            logger.info("|" + " " * 23 + "PIPELINE COMPLETED" + " " * 27 + "|")
            logger.info("+" + "-" * 68 + "+")
            logger.info(f"Duration: {duration}")
            logger.info(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.error("+" + "-" * 68 + "+")
            logger.error("|" + " " * 25 + "PIPELINE FAILED" + " " * 28 + "|")
            logger.error("+" + "-" * 68 + "+")
            logger.error(f"Error: {str(e)}")
            raise

        finally:
            if self.duckdb_conn:
                self.duckdb_conn.close()
                logger.info("DuckDB connection closed")


def parse_args(argv=None):
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Run the IPO analytics pipeline.")
    parser.add_argument(
        '--snapshot',
        type=date.fromisoformat,
        default=date.today(),
        help="Snapshot date (YYYY-MM-DD); raw files live in data/raw/<snapshot>/. Defaults to today.",
    )
    parser.add_argument(
        '--skip-scrape',
        action='store_true',
        help="Rebuild from an existing raw snapshot without scraping.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    setup_logging()
    pipeline = IPODataPipeline(snapshot=args.snapshot, skip_scrape=args.skip_scrape)
    pipeline.run()
