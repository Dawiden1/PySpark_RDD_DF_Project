from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder \
    .appName("Spark Project") \
    .config("spark.hadoop.hadoop.home.dir", "C:/Hadoop") \
    .getOrCreate()

datasource1 = spark.read.csv(
    "merged_data.csv",
    sep=";",        # separator
    header=False,    # nagłówki
    inferSchema=True # wykrycie typów kolumn
)

selected_columns = (datasource1.select(f.col("_c0").alias("player_id"),
                                       f.col("_c7").alias("player_positions"),
                                       f.col("_c10").alias("value_eur"),
                                       f.col("_c11").alias("wage_eur"),
                                       f.col("_c12").alias("age"),
                                       f.col("_c16").alias("league_id"),
                                       f.col("_c17").alias("club_team_id"),
                                       f.col("_c18").alias("club_name"),
                                       f.col("_c25").alias("nationality_name"))
                    )

agg_players = (selected_columns.groupBy("league_id", "club_team_id")
               .agg(f.count("player_id").alias("total_players"))
               .where(f.col("total_players") >= 11)
               )

agg_leagues = (agg_players.groupBy("league_id")
               .agg(f.count("club_team_id").alias("total_clubs"))
               .where(f.col("total_clubs") >= 10)
               )

reduced_data = (selected_columns
                .join(agg_leagues, on="league_id", how="inner")
                )



selected_columns.show()
selected_datasource1.show()