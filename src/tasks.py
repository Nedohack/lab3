from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, when, avg, count
from pyspark.sql.types import IntegerType


def full(free_slots: int) -> int:
    """
    UDF для визначення, чи заповнена станція.
    Повертає 1, якщо free_slots = 0, і 0, якщо free_slots > 0.
    """
    return 1 if free_slots == 0 else 0


def process_data(input_register: str, input_stations: str, output_path: str, threshold: float = 0.25):
    """
    Основна функція для обробки даних і обчислення критичності станцій.

    :param input_register: Шлях до файлу register.csv
    :param input_stations: Шлях до файлу stations.csv
    :param output_path: Шлях для збереження результатів
    :param threshold: Поріг критичності (за замовчуванням 0.25)
    """

    spark = SparkSession.builder.appName("StudentBikeCriticality").getOrCreate()


    spark.udf.register("full", full, IntegerType())


    register_df = spark.read.option("delimiter", "\t").option("header", "true").csv(input_register)


    print("Схема register.csv:")
    register_df.printSchema()
    print("Перші 5 рядків register.csv:")
    register_df.show(5)


    filtered_df = register_df.filter(~((col("used_slots") == 0) & (col("free_slots") == 0)))


    print("Після фільтрації (used_slots = 0 і free_slots = 0):")
    filtered_df.show(5)


    processed_df = filtered_df.select(
        col("station").cast("integer"),
        date_format(col("timestamp"), "EE").alias("dayofweek"),
        hour(col("timestamp")).alias("hour"),
        when(col("free_slots") == 0, 1).otherwise(0).alias("fullstatus")
    )


    print("Після створення полів dayofweek, hour, fullstatus:")
    processed_df.show(5)


    criticality_df = processed_df.groupBy("station", "dayofweek", "hour").agg(
        avg("fullstatus").alias("criticality"),
        count("*").alias("total_records")
    )


    print("Критичність для кожної пари (station, dayofweek, hour):")
    criticality_df.show(5)


    critical_df = criticality_df.filter(col("criticality") > threshold)


    stations_df = spark.read.option("delimiter", "\t").option("header", "true").csv(input_stations)


    print("Схема stations.csv:")
    stations_df.printSchema()
    print("Перші 5 рядків stations.csv:")
    stations_df.show(5)


    result_df = critical_df.join(
        stations_df,
        critical_df.station == stations_df.id,
        "inner"
    ).select(
        critical_df.station,
        critical_df.dayofweek,
        critical_df.hour,
        critical_df.criticality,
        stations_df.longitude,
        stations_df.latitude
    )


    sorted_df = result_df.orderBy(
        col("criticality").desc(),
        col("station").asc(),
        col("dayofweek").asc(),
        col("hour").asc()
    )


    print("Кінцевий результат перед збереженням:")
    sorted_df.show(5)

    sorted_df.write.mode("overwrite").option("header", "true").csv(output_path)


    spark.stop()