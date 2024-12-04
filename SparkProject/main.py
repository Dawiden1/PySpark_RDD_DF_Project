from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder \
    .appName("Spark Project") \
    .config("spark.hadoop.hadoop.home.dir", "C:/Hadoop") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

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

top_nationalities = (reduced_data.groupBy("nationality_name")
                          .agg(f.sum("value_eur").alias("sum_value_eur"),
                               f.avg("wage_eur").alias("avg_wage_eur"),
                               f.avg("age").alias("avg_age"),
                               f.count("player_id").alias("count_players"))
                          .orderBy(f.desc("avg_wage_eur"))
                          .withColumn("category",f.lit("nationality"))
                          .select(
                                  f.col("category"),
                                  f.col("nationality_name").alias("name"),
                                  f.col("sum_value_eur"),
                                  f.col("avg_wage_eur"),
                                  f.col("avg_age"),
                                  f.col("count_players"))
                          .limit(3))

"""top_clubs = (reduced_data.groupBy("club_name")
                          .agg(f.sum("value_eur").alias("sum_value_eur"),
                               f.avg("wage_eur").alias("avg_wage_eur"),
                               f.avg("age").alias("avg_age"),
                               f.count("player_id").alias("count_players"))
                          .orderBy(f.desc("avg_wage_eur"))
                          .withColumn("category",f.lit("club"))
                          .select(
                                  f.col("category"),
                                  f.col("nationality_name").alias("name"),
                                  f.col("sum_value_eur"),
                                  f.col("avg_wage_eur"),
                                  f.col("avg_age"),
                                  f.col("count_players"))
                          .limit(3))

top_leagues = (reduced_data.groupBy("league_name")
                          .agg(f.sum("value_eur").alias("sum_value_eur"),
                               f.avg("wage_eur").alias("avg_wage_eur"),
                               f.avg("age").alias("avg_age"),
                               f.count("player_id").alias("count_players"))
                          .orderBy(f.desc("avg_wage_eur"))
                          .withColumn("category",f.lit("league"))
                          .select(
                                  f.col("category"),
                                  f.col("nationality_name").alias("name"),
                                  f.col("sum_value_eur"),
                                  f.col("avg_wage_eur"),
                                  f.col("avg_age"),
                                  f.col("count_players"))
                          .limit(3))

fifaplayers = top_clubs.union(top_leagues).union(top_nationalities)
"""

top_nationalities.printSchema()

selected_columns.show()
top_nationalities.show()
