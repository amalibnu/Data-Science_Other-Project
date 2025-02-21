from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("nutrient").getOrCreate()

df = spark.read.parquet("/user/s2583755/Project/0000.parquet")

from pyspark.sql.functions import col, explode, regexp_extract, expr, count, avg, size, first, desc
from pyspark.sql.functions import regexp_replace, when, initcap
from pyspark.sql.types import NumericType
from pyspark.sql import Row

# Explode the nutriments array and filter for the 'sugar' nutrient
df_with_years = df.select(
    "categories_tags",
    "countries_tags",
    expr("filter(product_name, x -> x.lang = 'main')[0].text").alias("product_name"),
    "entry_dates_tags",
    explode("nutriments").alias("nutriment")
).filter(col("nutriment.name").like("%sugar%"))

# Add 'sugars_100g' and 'unit' as columns from the filtered 'nutriment' field
df_with_years = df_with_years.select(
    "categories_tags",
    "countries_tags",
    "product_name",
    col("entry_dates_tags"),
    col("nutriment.100g").alias("sugars_100g"),
    col("nutriment.unit").alias("sugars_unit")
)

# Extract year from entry_dates_tags
df_with_years = df_with_years.withColumn("Year_entry", regexp_extract(col("entry_dates_tags")[0], r"(\d{4})", 1))

# Exclude product with entry year > 2021
df_filtered = df_with_years.filter(col("Year_entry") <= 2021)

# Explode the categories_tags column
exploded_df = df_with_years.select(
    explode(col("categories_tags")).alias("category"),  # Create a row for each category
    col("countries_tags"),
    col("product_name"),
    col("sugars_100g"),
    col("Year_entry"),
)

# Group by category to calculate count product & avg sugar per 100g per category 
grouped_df = exploded_df.groupBy("category").agg(
    count("*").alias("Count_product"),                    # Count of rows (product) per category
    avg("sugars_100g").alias("Avg_sugars_100g")            # Average sugars_100g
).orderBy(desc("Count_product"))

# Create reference table 
# Step 1: Filter for Avg_sugars_100g > 5 (Remove non-sweet product category)
ref_category = grouped_df.filter(col("Avg_sugars_100g") > 5)

# Step 2: Limit to top 500 rows
ref_category = ref_category.limit(500)
categories_to_keep = [row['category'] for row in ref_category.select('category').collect()]

# Write the table as Top-500 sweet product category based on product count
ref_category.coalesce(1).write.mode("overwrite").option("header", "true").csv("project/final/q2_Top-500-Product-Category")

# Filter exploded_df with only top 500 most popular categories
filtered_exploded_df = exploded_df.filter(col('category').isin(categories_to_keep))

# Explode column "countries_tags"
exploded_countries_df = filtered_exploded_df.select(
    explode(col("countries_tags")).alias("country"),  # Create a row for each category
    col("category"),
    col("product_name"),
    col("sugars_100g"),
    col("Year_entry"),
)

# Fix "country" column naming before join with Diabetes Rate table
# Step 1: Remove prefixes like "en:" and standardize the case
exploded_countries_df_cleaned = exploded_countries_df.withColumn(
    "country_cleaned",
    initcap(regexp_replace(regexp_replace("country", r"^[a-z]{2}:", ""), "-", " "))
)

# Step 2: Apply manual correction 
exploded_countries_df_cleaned = exploded_countries_df_cleaned.withColumn(
    "country_cleaned",
    when(col("country_cleaned") == "United States", "United States of America")
    .when(col("country_cleaned") == "Czech Republic", "Czechia")
    .when(col("country_cleaned") == "Bolivia", "Bolivia (Plurinational State of)")
    .when(col("country_cleaned") == "Russia", "Russian Federation")
    .when(col("country_cleaned") == "Cote D Ivoire", "Côte d'Ivoire")
    .when(col("country_cleaned") == "French Guiana", "Guyana")
    .when(col("country_cleaned") == "Venezuela", "Venezuela (Bolivarian Republic of)")
    .when(col("country_cleaned") == "South Korea", "Republic of Korea")
    .when(col("country_cleaned") == "Moldova", "Republic of Moldova")
    .when(col("country_cleaned") == "Vietnam", "Viet Nam")
    .when(col("country_cleaned") == "Iran", "Iran (Islamic Republic of)")
    .when(col("country_cleaned") == "Macau", "Macao")
    .when(col("country_cleaned") == "Palestinian Territories", "State of Palestine")
    .when(col("country_cleaned") == "Deutschland", "Germany")
    .when(col("country_cleaned") == "Francia", "France")
    .when(col("country_cleaned") == "Frankreich", "France")
    .when(col("country_cleaned") == "Belgique", "Belgium")
    .when(col("country_cleaned") == "Schweiz", "Switzerland")
    .when(col("country_cleaned") == "Etats Unis", "United States of America")
    .when(col("country_cleaned") == "Suisse", "Switzerland")
    .when(col("country_cleaned") == "Espagne", "Spain")
    .when(col("country_cleaned") == "Allemagne", "Germany")
    .when(col("country_cleaned") == "Estados Unidos", "United States of America")
    .when(col("country_cleaned") == "Turkiye", "Iran (Turkey)")
    .when(col("country_cleaned") == "Scotland", "United Kingdom")
    .when(col("country_cleaned") == "Belgie", "Belgium")
    .when(col("country_cleaned") == "England", "United Kingdom")
    .when(col("country_cleaned") == "Suiza", "Switzerland")
    .when(col("country_cleaned") == "Nederland", "Netherlands")
    .when(col("country_cleaned") == "Belgien", "Belgium")
    .when(col("country_cleaned") == "Belgica", "Belgium")
    .when(col("country_cleaned") == "Belgie", "Belgium")
    .when(col("country_cleaned") == "Algerie", "Algeria")
    .when(col("country_cleaned") == "Irland", "Ireland")
    .when(col("country_cleaned") == "Belgien", "Belgium")
    .otherwise(col("country_cleaned"))  # Keep original value if no match
)

# Select columns in the desired order
exploded_countries_df_cleaned = exploded_countries_df_cleaned.select(
    "country_cleaned", *[col for col in exploded_countries_df_cleaned.columns if col != "country_cleaned"]
)

# Group by country and category, calculate average sugars_100g and count products
pivot_table = (
    exploded_countries_df_cleaned
    .groupBy("country_cleaned", "category")
    .agg(
        avg("sugars_100g").alias("avg_sugars_100g"),
        count("product_name").alias("count_product")
    )
)

# Transform to move category as column
pivot_df = pivot_table.groupBy("country_cleaned").pivot("category", categories_to_keep).agg({"avg_sugars_100g": "avg"})

# Write cleaned preprocessed table
pivot_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("project/final/q3_correlation/country_category_preprocessed")