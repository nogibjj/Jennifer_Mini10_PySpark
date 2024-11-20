# main.py
from mylib.lib import PySparkProcessor
from pyspark.sql.functions import desc
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # Initialize processor
    processor = PySparkProcessor()
    try:
        # Local file path - updated to use goose.csv
        file_path = os.path.join("data", "goose.csv")

        # Load data
        logger.info("Loading baseball statistics data...")
        df = processor.load_data(file_path)
        logger.info(f"Loaded {df.count()} records")

        # Apply transformations
        logger.info("Calculating advanced metrics...")
        transformed_df = processor.transform_data(df)

        logger.info("\nSample of transformed data with new metrics:")
        transformed_df.select(
            "name",
            "year",
            "team",
            "success_rate",
            "performance_above_replacement",
            "opportunity_conversion",
            "league_adjusted_performance",
            "yearly_rank",
        ).orderBy(desc("league_adjusted_performance")).show(5)

        # Run SQL analysis
        logger.info("Performing historical analysis...")
        analysis_results = processor.run_sql_analysis(transformed_df)

        logger.info("\nTeam Performance by Era:")
        analysis_results["team_era_stats"].show(5)

        logger.info("\nTop Performers by Era:")
        analysis_results["top_performers"].show(5)

        logger.info("\nLeague Evolution:")
        analysis_results["league_trends"].show(5)

        # Save results
        output_path = os.path.join("data")
        logger.info(f"Saving analysis results to {output_path}...")
        processor.save_results(analysis_results, output_path)
        logger.info("Analysis completed successfully")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        processor.stop_spark()


if __name__ == "__main__":
    main()
