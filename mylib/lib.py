from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round,
    desc,
    when,
    row_number,
)
from pyspark.sql.window import Window


class PySparkProcessor:
    def __init__(self):
        """Initialize Spark session"""
        self.spark = (
            SparkSession.builder.appName("Baseball Relief Pitcher Analysis")
            .config("spark.sql.repl.eagerEval.enabled", True)
            .getOrCreate()
        )

    def load_data(self, file_path):
        """Load baseball statistics data from local CSV file"""
        return (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )

    def transform_data(self, df):
        """Transform baseball statistics with relevant metrics"""
        # Calculate relief pitcher effectiveness metrics
        transformed_df = df.withColumns(
            {
                # Success rate (goose eggs vs broken eggs)
                "success_rate": when(
                    (col("goose_eggs") + col("broken_eggs")) > 0,
                    round(
                        col("goose_eggs") / (col("goose_eggs") + col("broken_eggs")), 3
                    ),
                ).otherwise(0.0),
                # Performance above replacement
                "performance_above_replacement": round(
                    col("gwar") - col("replacement_gpct"), 3
                ),
                # Opportunity conversion (including mehs)
                "opportunity_conversion": when(
                    (col("goose_eggs") + col("broken_eggs") + col("mehs")) > 0,
                    round(
                        col("goose_eggs")
                        / (col("goose_eggs") + col("broken_eggs") + col("mehs")),
                        3,
                    ),
                ).otherwise(0.0),
                # League adjusted performance
                "league_adjusted_performance": round(
                    (col("gwar") * col("league_average_gpct"))
                    / col("replacement_gpct"),
                    3,
                ),
            }
        )

        # Add rankings within each year
        window_spec = Window.partitionBy("year").orderBy(desc("gwar"))
        final_df = transformed_df.withColumn(
            "yearly_rank", row_number().over(window_spec)
        )

        return final_df

    def run_sql_analysis(self, df):
        """Perform comprehensive baseball statistics analysis"""
        df.createOrReplaceTempView("pitcher_stats")

        # Analysis 1: Team performance by era (decades)
        team_era_stats = self.spark.sql(
            """
            SELECT 
                team,
                FLOOR(year/10)*10 as decade,
                COUNT(DISTINCT name) as num_pitchers,
                SUM(goose_eggs) as total_goose_eggs,
                SUM(broken_eggs) as total_broken_eggs,
                ROUND(AVG(gwar), 3) as avg_gwar,
                ROUND(SUM(goose_eggs) * 100.0 / 
                    (SUM(goose_eggs) + SUM(broken_eggs)), 2) as team_success_rate
            FROM pitcher_stats
            GROUP BY team, FLOOR(year/10)*10
            HAVING COUNT(*) > 10
            ORDER BY decade, team_success_rate DESC
        """
        )

        # Analysis 2: Top performers by era
        top_performers = self.spark.sql(
            """
            WITH ranked_pitchers AS (
                SELECT 
                    name,
                    year,
                    team,
                    goose_eggs,
                    broken_eggs,
                    gwar,
                    DENSE_RANK() OVER (
                        PARTITION BY FLOOR(year/10)*10 
                        ORDER BY gwar DESC
                    ) as era_rank
                FROM pitcher_stats
                WHERE goose_eggs + broken_eggs > 10
            )
            SELECT *
            FROM ranked_pitchers
            WHERE era_rank <= 5
            ORDER BY year DESC, era_rank
        """
        )

        # Analysis 3: League evolution
        league_trends = self.spark.sql(
            """
            SELECT 
                year,
                league,
                COUNT(DISTINCT name) as num_pitchers,
                ROUND(AVG(league_average_gpct), 3) as avg_league_gpct,
                ROUND(AVG(gwar), 3) as avg_gwar,
                ROUND(AVG(replacement_gpct), 3) as avg_replacement_gpct
            FROM pitcher_stats
            GROUP BY year, league
            ORDER BY year DESC, league
        """
        )

        return {
            "team_era_stats": team_era_stats,
            "top_performers": top_performers,
            "league_trends": league_trends,
        }

    def save_results(self, results_dict, base_output_path):
        """Save processed data to separate directories"""
        for name, df in results_dict.items():
            output_path = f"{base_output_path}/{name}"
            df.write.mode("overwrite").parquet(output_path)

    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
