#import library
from pyspark.sql.functions import col, explode, avg, desc, regexp_extract, when, initcap, regexp_replace
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("europe") \
    .getOrCreate()

#load data from parquet to spark dataframe
df = spark.read.parquet('/user/s2583755/Project/0000.parquet')

#column selection
df2 = df.select("product_name", "entry_dates_tags", "countries_tags","categories", 'categories_tags', "nutriments", "additives_n", "nutriscore_grade")

#explode column
df_explode = df2.withColumn("nutriments_exp", explode(col("nutriments")))\
    .withColumn("categories_exp", explode(col("categories_tags")))\
    .withColumn("countries_exp", explode(col("countries_tags")))\
    .withColumn("entry_dates_exp", explode(col("entry_dates_tags")))
    #.withColumn("productname", explode(col("product_name")))

df_explode.select("nutriments_exp", "product_name").show(3, truncate = False)

df_explode2 = df_explode.filter(col("categories_exp").startswith("en:"))\
    .withColumn("categories_clean", regexp_replace(col("categories_exp"), r"^en:", ""))\
    .withColumn("countries_clean", regexp_replace(col("countries_exp"), r"^en:", ""))\
    .withColumn("categories_clean", initcap(col("categories_clean")))\
    .withColumn("countries_clean", initcap(col("countries_clean")))\
    .withColumn("year", regexp_extract(col("entry_dates_exp"), r"(\d{4})", 1))


finaldf = df_explode2.select("additives_n", "nutriscore_grade", "year", 
"categories_clean", "countries_clean",col("nutriments_exp.name").alias("nutriments_name"), 
col("nutriments_exp.value").alias("nutriments_value"),
col("nutriments_exp.100g").alias("nutriments_per100g"))\
.filter(col("nutriments_name").rlike('(?i)sugar'))

#clean countries
# Define the mapping dictionary
country_mapping = {
    'Czech-republic': 'Czech Republic',
    'Czech-republic-ƒçe≈°tina': 'Czech Republic',
    'Fr:france': 'France',
    'Fr:francia': 'France',
    'Fr:deutschland': 'Deutschland',
    'Fr:nederland': 'Netherlands',
    'Fr:frankrijk': 'France',
    'Fr:angleterre': 'Angleterre',
    'Fr:frankreich': 'France',
    'Fr:belgica': 'Belgium',
    'Fr:belgien': 'Belgium',
    'Fr:noruega': 'Norway',
    'Fr:dom-tom': 'Dom-Tom',
    'Fr:spanje': 'Spain',
    'Belgium-francais': 'Belgium',
    'Belgium-nederlands': 'Belgium',
    'Francia': 'France',
    'Fr:france-la-reunion': 'France',
    'Francia-espana': 'France',
    'France-spain': 'France',
    'Franca': 'France',
    'Fr:france-üá®üáµüá´üá∑': 'France',
    'Ca:franca': 'France',
    'France-francais': 'France',
    'East-germany': 'Germany',
    'Germany-deutsch': 'Germany',
    'Netherlands-nederlands': 'Netherlands',
    'Italy-italiano': 'Italy',
    'Spain-espanol': 'Spain',
    'Portugal-portugues': 'Portugal',
    'Sweden-svenska': 'Sweden',
    'Norway-norsk': 'Norway',
    'Denmark-dansk': 'Denmark',
    'Austria-deutsch': 'Austria',
    'Switzerland-deutsch': 'Switzerland',
    'Switzerland-francais': 'Switzerland',
    'Poland-polski': 'Poland',
    'Hungary-magyar': 'Hungary',
    'Greece-ŒµŒªŒªŒ∑ŒΩŒπŒ∫Œ¨': 'Greece',
    'Finland-suomi': 'Finland',
    'Romania-romanƒÉ': 'Romania',
    'Bulgaria-–±—ä–ª–≥–∞—Ä—Å–∫–∏': 'Bulgaria',
    'Croatia-hrvatski': 'Croatia',
    'Slovenia-slovene': 'Slovenia',
    'Estonia-eesti': 'Estonia',
    'Latvia-latvie≈°u': 'Latvia',
    'Lithuania-lietuvi≈≥': 'Lithuania',
    'Cyprus-ŒµŒªŒªŒ∑ŒΩŒπŒ∫Œ¨': 'Cyprus'   
}

# Broadcast the mapping dictionary for better performance
broadcast_mapping = F.create_map([F.lit(x) for item in country_mapping.items() for x in item])


# Create a new column `countries_clean2` based on the mapping dictionary
finaldf_cleaned_countries = finaldf.withColumn(
    "countries_clean2", 
    F.coalesce(broadcast_mapping[col("countries_clean")], col("countries_clean"))  # Use the mapping or default to original value
)

#filter just Europe countries
european_countries = [
    "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus", 
    "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Cyprus", 
    "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Georgia", 
    "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", 
    "Kazakhstan", "Kosovo", "Latvia", "Liechtenstein", "Lithuania", 
    "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", 
    "North Macedonia", "Norway", "Poland", "Portugal", "Romania", 
    "San Marino", "Serbia", "Slovakia", "Slovenia", "Spain", "Sweden", 
    "Switzerland", "Turkey", "Ukraine", "United Kingdom", "Vatican City"
]

finaldf_cleaned_countriesEurope = finaldf_cleaned_countries.filter(col("countries_clean2").isin(european_countries))

#top 500 product category
filter_Europe = (finaldf_cleaned_countriesEurope.groupBy("categories_clean")
             .agg(F.count("categories_clean").alias("count"))
             .orderBy(F.col("count").desc())
             .limit(500))

 #collect top 500 category list
 #list_category = [row["categories_clean"] for row in filter_Europe.select("categories_clean").collect()]  
list_category = [
    row["categories_clean"] for row in filter_Europe.select("categories_clean").collect()
] 

#filter data based on top 500 category
finaldf_cleaned_countriesEurope2 = finaldf_cleaned_countriesEurope.filter(F.col("categories_clean").isin(list_category))

#save the result to a CSV or other format
path = "/user/s2583755/Project/Europe2"
finaldf_cleaned_countriesEurope2.coalesce(1).write.csv(path,header=True)


