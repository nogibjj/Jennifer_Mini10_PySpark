# test_main.py
import pytest
from mylib.lib import PySparkProcessor
import os


@pytest.fixture(scope="session")
def spark_processor():
    processor = PySparkProcessor()
    yield processor
    processor.stop_spark()


def test_load_data(spark_processor):
    """Test if data can be loaded correctly"""
    file_path = os.path.join("data", "goose.csv")
    df = spark_processor.load_data(file_path)

    # Check if data is loaded
    assert df.count() > 0
    # Check if essential columns exist
    expected_columns = ["name", "year", "team", "goose_eggs", "broken_eggs", "gwar"]
    assert all(col in df.columns for col in expected_columns)


def test_transform_data(spark_processor):
    """Test if transformations work correctly"""
    # Load sample data
    file_path = os.path.join("data", "goose.csv")
    df = spark_processor.load_data(file_path)

    # Apply transformations
    transformed_df = spark_processor.transform_data(df)

    # Check if new columns were added
    new_columns = [
        "success_rate",
        "performance_above_replacement",
        "opportunity_conversion",
        "league_adjusted_performance",
        "yearly_rank",
    ]
    assert all(col in transformed_df.columns for col in new_columns)


def test_sql_analysis(spark_processor):
    """Test if SQL analysis produces expected results"""
    # Load and transform data
    file_path = os.path.join("data", "goose.csv")
    df = spark_processor.load_data(file_path)
    transformed_df = spark_processor.transform_data(df)

    # Run analysis
    results = spark_processor.run_sql_analysis(transformed_df)

    # Check if all expected analyses are present
    expected_analyses = ["team_era_stats", "top_performers", "league_trends"]
    assert all(analysis in results for analysis in expected_analyses)

    # Check if analyses have data
    for analysis_name, analysis_df in results.items():
        assert analysis_df.count() > 0
