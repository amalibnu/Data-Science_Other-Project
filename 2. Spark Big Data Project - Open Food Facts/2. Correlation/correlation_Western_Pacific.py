from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_extract, expr
from pyspark.sql.functions import count, avg, size, first, desc
from pyspark.sql.functions import regexp_replace, when, initcap
from pyspark.sql.types import NumericType
from pyspark.sql import Row


# Start spark Session
spark = SparkSession.builder.appName("correlation_Western Pacific").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

pivot_df = spark.read.csv("/user/s3055876/project/final/q3_correlation/country_category_preprocessed/country_category_preprocessed.csv", header = True)
diabetes_rate = spark.read.csv("/user/s3055876/project/Diabetes_Rate_2021_withRegionv2.csv", header = True)

# Perform inner join
joined_df = pivot_df.join(
    diabetes_rate,
    pivot_df["country_cleaned"] == diabetes_rate["Country/Territory"],
    "inner"  # Inner join
)

# Change Diabetes_Percentage data format to numeric
joined_df = joined_df.withColumn(
    "Diabetes_Percentage", col("Diabetes Percentage").cast("double")
)

# Drop duplicate column
joined_df = joined_df.drop("Country/Territory","country", "Diabetes Percentage")

# Get the list of all columns
all_columns = joined_df.columns

# Move "Diabetes_Percentage" and "Region" to the second position
reordered_columns = ["country_cleaned", "Region", "Diabetes_Percentage"] + [col for col in all_columns if col not in ["country_cleaned", "Region", "Diabetes_Percentage"]]

# Reorder the DataFrame
reordered_df = joined_df.select(*reordered_columns)

# Filter for Region == "Europe"
reordered_df = reordered_df.filter(col("Region") == "Western Pacific")

# Specify columns to exclude
exclude_columns = {"country_cleaned", "Region", "Diabetes_Percentage"}

# Dynamically select columns to cast
columns_to_cast = [field.name for field in reordered_df.schema.fields if field.name not in exclude_columns]

# Cast the columns to double
for column in columns_to_cast:
    reordered_df = reordered_df.withColumn(column, col(column).cast("double"))

# Calculate correlations
numeric_cols = [column for column in reordered_df.columns if column not in exclude_columns]


correlations = {}
for column in numeric_cols:
    if isinstance(reordered_df.schema[column].dataType, NumericType):
        corr_value = reordered_df.stat.corr(column, "Diabetes_Percentage")
        correlations[column] = corr_value

# Convert dictionary to list of Rows
correlation_rows = [Row(Column=col, Correlation=correlation) for col, correlation in correlations.items()]

# Create a PySpark DataFrame
correlation_spark_df = spark.createDataFrame(correlation_rows)

# Show the results sorted by correlation
correlation_spark_df.orderBy("Correlation", ascending=False).show(10)

correlation_spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("project/final/q3_correlation/Western_Pacific")