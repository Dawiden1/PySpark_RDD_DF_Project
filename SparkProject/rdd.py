from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext

spark = SparkSession.builder \
    .appName("Spark Project") \
    .config("spark.hadoop.hadoop.home.dir", "C:/Hadoop") \
    .getOrCreate()

sc = SparkContext.getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

# Wczytywanie danych
datasource1 = spark.read.csv("dataset1.csv", sep=";", header=False, inferSchema=True)
datasource4 = spark.read.csv("dataset4.csv", sep=";", header=False, inferSchema=True)

# Transformacja na RDD i mapowanie kolumn
datasource1_rdd = datasource1.rdd.map(lambda row: (row[0], row[7], row[10], row[11], row[12], row[16], row[17], row[18], row[25]))
datasource4_rdd = datasource4.rdd.map(lambda row: (row[0], row[1], row[2]))

# Filtrowanie i agregowanie danych
agg_players_rdd = datasource1_rdd.map(lambda row: ((row[5], row[6]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda row: row[1] >= 11)

agg_leagues_rdd = agg_players_rdd.map(lambda row: (row[0][0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda row: row[1] >= 10)

# Łączenie danych
reduced_data_rdd = datasource1_rdd.map(lambda row: (row[5], row)) \
    .join(agg_leagues_rdd) \
    .map(lambda row: row[1][0])

# Agregowanie danych według klubów
top_clubs_rdd = reduced_data_rdd.map(lambda row: (row[7], (row[2], row[3], row[4], row[0], row[1]))) \
    .groupByKey() \
    .mapValues(lambda values: (
        sum([v[0] for v in values]),
        sum([v[1] for v in values]) / len(values),
        sum([v[2] for v in values]) / len(values),
        len(values),
        list(set([p for value in values for p in value[3].split(',')]))
    )) \
    .sortBy(lambda row: -row[1][1]) \
    .map(lambda row: ("club", row[0], round(row[1][0], 0), round(row[1][1], 3), int(round(row[1][2], 0)), row[1][3], row[1][4])) \
    .take(3)

# Agregowanie danych według lig
leagues_columns_rdd = datasource4_rdd.map(lambda row: (row[0], (row[1], row[2])))

top_leagues_rdd = reduced_data_rdd.map(lambda row: (row[5], (row[2], row[3], row[4], row[0], row[1]))) \
    .join(leagues_columns_rdd) \
    .map(lambda row: (row[1][1][0], (row[1][0][0], row[1][0][1], row[1][0][2], row[1][0][3], row[1][0][4]))) \
    .groupByKey() \
    .mapValues(lambda values: (
        sum([v[0] for v in values]),
        sum([v[1] for v in values]) / len(values),
        sum([v[2] for v in values]) / len(values),
        len(values),
        list(set([p for value in values for p in value[3].split(',')]))
    )) \
    .sortBy(lambda row: -row[1][1]) \
    .map(lambda row: ("league", row[0], round(row[1][0], 0), round(row[1][1], 3), int(round(row[1][2], 0)), row[1][3], row[1][4])) \
    .take(3)

# Agregowanie danych według narodowości
top_nationalities_rdd = reduced_data_rdd.map(lambda row: (row[8], (row[2], row[3], row[4], row[0], row[1]))) \
    .groupByKey() \
    .mapValues(lambda values: (
        sum([v[0] for v in values]),
        sum([v[1] for v in values]) / len(values),
        sum([v[2] for v in values]) / len(values),
        len(values),
        list(set([p for value in values for p in value[3].split(',')]))
    )) \
    .sortBy(lambda row: -row[1][1]) \
    .map(lambda row: ("nationality", row[0], round(row[1][0], 0), round(row[1][1], 3), int(round(row[1][2], 0)), row[1][3], row[1][4])) \
    .take(3)

# Połączenie danych w jedną strukturę
fifa_players_rdd = sc.parallelize(top_clubs_rdd + top_leagues_rdd + top_nationalities_rdd)

# Przekształcenie do DataFrame
fifa_players_df = fifa_players_rdd.toDF(["category", "name", "sum_value_eur", "avg_wage_eur", "avg_age", "count_players", "player_positions"])

# Utworzenie widoku tymczasowego i wyświetlenie wyników
fifa_players_df.createOrReplaceTempView("df_result_table")
fifa_players_df.printSchema()
fifa_players_df.show(truncate=False)
