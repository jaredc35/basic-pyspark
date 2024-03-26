# Import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, min, max, avg, when
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Import streamlit for visualization
import streamlit as st

spark = (
    SparkSession.builder.master("local[2]").appName("PySpark_Tutorial").getOrCreate()
)

csv_file = "./stock_data/stocks_price_final.csv"
data_schema = [  # Establish a schema for the data
    StructField("_c0", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("data", DateType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("adjusted", DoubleType(), True),
    StructField("market_cap", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("exchange", StringType(), True),
]

final_struc = StructType(fields=data_schema)
data = spark.read.csv(csv_file, sep=",", header=True, schema=final_struc)


def main():
    sec_df = (
        data.select(["sector", "open", "close", "adjusted"])
        .groupBy("sector")
        .mean()
        .toPandas()
    )

    ind = list(range(12))

    ind.pop(6)

    sec_df.iloc[ind, :].plot(
        kind="bar",
        x="sector",
        y=sec_df.columns.tolist()[1:],
        figsize=(12, 6),
        ylabel="Stock Price",
        xlabel="Sector",
    )
    plt.show()
    #

    # print(data.schema) # Displays schema wrt their structure type
    # print(data.dtypes) # List of tuples showing each column and element type
    # data.printSchema()

    # print(data.head(3)) # defaults to return first 20 items
    # print(data.show(3)) # Prettier output than head
    # print(data.first())  # Very similar to head, but only returns the first
    # print(data.describe().show()) # Pretty output of description of each column

    # Edit column names
    # data = data.withColumn("date", data.data)
    # data = data.withColumnRenamed("date", "Data_changed")
    # data = data.drop("Data_changed")
    # data.show(2)

    # Different ways to deal with missing values
    # Drop them entirely
    # data.na.drop()

    # Replace missing values with mean
    # data.na.fill(data.select(avg(data["open"])).collect()[0][0])

    # Replace missing values with a specific value
    # new_value = 0.
    # data.na.fill(new_value)

    # Select specific columns to show
    # data.select(["open", "close", "adjusted"]).show(5)

    # Filter by specific dates
    # data.filter(
    #     (col("data") >= lit("2020-01-05")) & (col("data") <= lit("2020-01-31"))
    # ).show(5)

    # Between filter for adjusted price
    # data.filter(data.adjusted.between(100.0, 500.0)).show(5)

    # When - Returns 0 or 1 depending on given condition
    # data.select("open", "close", when(data.adjusted >= 200.0, 1).otherwise(0)).show(5)

    # data.select(
    #     "sector", data.sector.rlike("^[B,C]").alias("Sectors starting with B or C")
    # ).distinct().show()

    # data.select(["industry", "open", "close", "adjusted"]).groupBy(
    #     "industry"
    # ).mean().show()

    # data.filter(
    #     (col("data") >= lit("2019-01-02")) & (col("data") <= lit("2020-01-31"))
    # ).groupBy("sector").agg(
    #     min("data").alias("From"),
    #     max("data").alias("To"),
    #     min("open").alias("Minimum Opening"),
    #     max("open").alias("Maximum Opening"),
    #     avg("open").alias("Average Opening"),
    #     min("close").alias("Minimum Closing"),
    #     max("close").alias("Maximum Closing"),
    #     avg("close").alias("Average Closing"),
    #     min("adjusted").alias("Minimum Adjusted Closing"),
    #     max("adjusted").alias("Maximum Adjusted Closing"),
    #     avg("adjusted").alias("Average Adjusted Closing"),
    # ).show(
    #     truncate=False
    # )


if __name__ == "__main__":
    main()
