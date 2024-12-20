from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder \
    .appName("Spark Project") \
    .config("spark.hadoop.hadoop.home.dir", "C:/Hadoop") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

datasource1 = spark.read.csv(
    "dataset1.csv",
    sep=";",  # separator
    header=False,  # nagłówki
    inferSchema=True  # wykrycie typów kolumn
)

datasource4 = spark.read.csv(
    "dataset4.csv",
    sep=";",  # separator
    header=False,  # nagłówki
    inferSchema=True  # wykrycie typów kolumn
)

leagues_columns = (datasource4.select(f.col("_c0").alias("league_id"),
                                      f.col("_c1").alias("league_name"),
                                      f.col("_c2").alias("league_level")))

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

top_clubs = (reduced_data.groupBy("club_name")
             .agg(f.sum("value_eur").alias("sum_value_eur"),
                    f.avg("wage_eur").alias("avg_wage_eur"),
                    f.avg("age").alias("avg_age"),
                    f.count("player_id").alias("count_players"),
                    f.collect_list("player_positions").alias("player_positions"))
                .orderBy(f.desc("avg_wage_eur"))
                .select(
    f.lit("club").alias("category"),
    f.col("club_name").alias("name"),
    f.format_number(f.col("sum_value_eur"), 0).alias("sum_value_eur"),
    f.round(f.col("avg_wage_eur") / 1000, 3).alias("avg_wage_eur"),
    f.round(f.col("avg_age"), 0).cast("int").alias("avg_age"),
    f.col("count_players").alias("count_players"),
    f.array_distinct(f.col("player_positions")).alias("player_positions"))
                .limit(3))

collected_player_positions = reduced_data. \
    select("club_name", f.explode(f.split(f.col("player_positions"), ",\\s*")).alias("player_position")). \
    groupBy("club_name"). \
    agg(f.collect_set("player_position").alias("player_positions")). \
    select("club_name", "player_positions")

top_clubs = (
    top_clubs
    .drop("player_positions")
    .join(collected_player_positions, top_clubs["name"] == collected_player_positions["club_name"], how="inner")
    .drop("club_name")
    .orderBy("avg_wage_eur", ascending=False)
)

top_leagues = (reduced_data.join(leagues_columns, 'league_id')
               .groupBy("league_name")
               .agg(f.sum("value_eur").alias("sum_value_eur"),
                    f.avg("wage_eur").alias("avg_wage_eur"),
                    f.avg("age").alias("avg_age"),
                    f.count("player_id").alias("count_players"),
                    f.collect_list("player_positions").alias("player_positions"))
               .orderBy(f.desc("avg_wage_eur"))
               .select(
    f.lit("league").alias("category"),
    f.col("league_name").alias("name"),
    f.format_number(f.col("sum_value_eur"), 0).alias("sum_value_eur"),
    f.round(f.col("avg_wage_eur") / 1000, 3).alias("avg_wage_eur"),
    f.round(f.col("avg_age"), 0).cast("int").alias("avg_age"),
    f.col("count_players").alias("count_players"),
    f.array_distinct(f.col("player_positions")).alias("player_positions"))
               .limit(3))

reduced_data_with_league = reduced_data.join(leagues_columns, on="league_id", how="inner")

collected_league_positions = reduced_data_with_league. \
    select("league_name", f.explode(f.split(f.col("player_positions"), ",\\s*")).alias("player_position")). \
    groupBy("league_name"). \
    agg(f.collect_set("player_position").alias("player_positions")). \
    select("league_name", "player_positions")

top_leagues = (
    top_leagues
    .drop("player_positions")
    .join(collected_league_positions, top_leagues["name"] == collected_league_positions["league_name"], how="inner")
    .drop("league_name")
    .orderBy("avg_wage_eur", ascending=False)
)

top_nationalities = (reduced_data.groupBy("nationality_name")
               .agg(f.sum("value_eur").alias("sum_value_eur"),
                    f.avg("wage_eur").alias("avg_wage_eur"),
                    f.avg("age").alias("avg_age"),
                    f.count("player_id").alias("count_players"),
                    f.collect_list("player_positions").alias("player_positions"))
               .orderBy(f.desc("avg_wage_eur"))
               .select(
    f.lit("nationality").alias("category"),
    f.col("nationality_name").alias("name"),
    f.format_number(f.col("sum_value_eur"), 0).alias("sum_value_eur"),
    f.round(f.col("avg_wage_eur") / 1000, 3).alias("avg_wage_eur"),
    f.round(f.col("avg_age"), 0).cast("int").alias("avg_age"),
    f.col("count_players").alias("count_players"),
    f.array_distinct(f.col("player_positions")).alias("player_positions"))
                .limit(3))

collected_nationality_positions = reduced_data. \
    select("nationality_name", f.explode(f.split(f.col("player_positions"), ",\\s*")).alias("player_position")). \
    groupBy("nationality_name"). \
    agg(f.collect_set("player_position").alias("player_positions")). \
    select("nationality_name", "player_positions")

top_nationalities = (
    top_nationalities
    .drop("player_positions")
    .join(collected_nationality_positions, top_nationalities["name"] == collected_nationality_positions["nationality_name"], how="inner")
    .drop("nationality_name")
    .orderBy("avg_wage_eur", ascending=False)
)

fifa_players = top_clubs.union(top_leagues).union(top_nationalities)

#fifa_players.createOrReplaceTempView("df_result_table")

fifa_players.printSchema()
fifa_players.show(truncate=False)
#fifa_players.select("player_positions").show(truncate=False)