from zipfile import ZipFile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, hash, current_timestamp, trim, regexp_replace, coalesce, lit

# TODO: Refactoring and parametrize of the script


def add_column_prefix(df, prefix, except_columns: tuple = ('hash_key', )):
    for column in df.columns:
        if column not in except_columns:
            df = df.withColumnRenamed(column, prefix + '_' + column)
    return df


def key_cleaner(column):
    return lower(trim(regexp_replace(coalesce(col(column), lit("")), "\\s+|\\r|\\n", "")))


def extract():
    with ZipFile("datasets.zip", mode="r") as archive:
        archive.extractall('./datasets')


def main():
    extract()

    # Initialize Spark session
    spark = SparkSession.builder.appName("DataIntegration").master("local[*]").getOrCreate()

    facebook_df = spark.read.csv('./datasets/facebook_dataset.csv',
                                 sep=',',
                                 header=True,
                                 quote='"',
                                 escape='\\',
                                 inferSchema=True,
                                 encoding='utf-8')
    google_df = spark.read.csv('./datasets/google_dataset.csv',
                               sep=',',
                               header=True,
                               quote='"',
                               escape='\\',
                               inferSchema=True,
                               encoding='utf-8')
    website_df = spark.read.csv('./datasets/website_dataset.csv',
                                sep=';',
                                header=True,
                                quote='"',
                                escape='\\',
                                encoding='utf-8',
                                inferSchema=True,
                                multiLine=True)

    # Rename columns for joining
    facebook_df = facebook_df.withColumnRenamed("name", "company_name")\
        .withColumnRenamed("country_name", "country_name")\
        .withColumnRenamed("city", "city")\
        .withColumnRenamed('categories', 'category')
    google_df = google_df.withColumnRenamed("name", "company_name")\
        .withColumnRenamed("country_name", "country_name")\
        .withColumnRenamed("city", "city")
    website_df = website_df.withColumnRenamed("legal_name", "company_name")\
        .withColumnRenamed("main_country", "country_name")\
        .withColumnRenamed("main_city", "city")\
        .withColumnRenamed('s_category', 'category')

    facebook_selected_columns = [
        col("category"),
        col("address"),
        col("country_name"),
        col("country_code"),
        col("city"),
        col("email"),
        col("company_name"),
        col("phone"),
        col("phone_country_code"),
        col("region_name"),
        col("zip_code"),
        col("domain")  # , col("description")
    ]
    google_selected_columns = [
        col("category"),
        col("address"),
        col("country_name"),
        col("country_code"),
        col("city"),
        col("company_name"),
        col("phone"),
        col("phone_country_code"),
        col("region_name"),
        col("zip_code"),
        col("domain")  # , col("text")
    ]
    website_selected_columns = [
        col("root_domain"),
        col("company_name"),
        col("country_name"),
        col("city"),
        col("main_region"),
        col("category")
    ]

    facebook_df = facebook_df.select(facebook_selected_columns)
    google_df = google_df.select(google_selected_columns)
    website_df = website_df.select(website_selected_columns)

    # Creating hash key
    hash_expr = hash(concat_ws("", key_cleaner("company_name"), key_cleaner("country_name"), key_cleaner("city")))
    facebook_df = facebook_df.withColumn("hash_key", hash_expr).repartition("hash_key")
    google_df = google_df.withColumn("hash_key", hash_expr).repartition("hash_key")
    website_df = website_df.withColumn("hash_key", hash_expr).repartition("hash_key")

    facebook_df = add_column_prefix(facebook_df, 'facebook')
    google_df = add_column_prefix(google_df, 'google')
    website_df = add_column_prefix(website_df, 'website')

    # print(f"Facebook count: {facebook_df.distinct().count()}")
    # print(f"Google count: {google_df.distinct().count()}")
    # print(f"Website count: {website_df.distinct().count()}")

    # Joining datasets
    full_df = google_df.join(facebook_df, ["hash_key"], "inner").join(website_df, ["hash_key"], "inner")

    full_df = full_df.select(
        col("hash_key"),
        coalesce(col("google_company_name"),
                 col("facebook_company_name"),
                 col("website_company_name")).alias("company_name"),
        concat_ws(" | ",
                  col("google_category"),
                  col("facebook_category"),
                  col("website_category")).alias("category"),
        coalesce(col("google_address"),
                 col("facebook_address")).alias("address"),
        coalesce(col("google_country_name"),
                 col("facebook_country_name"),
                 col("website_country_name")).alias("country_name"),
        coalesce(col("google_country_code"),
                 col("facebook_country_code")).alias("country_code"),
        coalesce(col("google_city"),
                 col("facebook_city"),
                 col("website_city")).alias("city"),
        coalesce(col("google_phone"),
                 col("facebook_phone")).alias("phone"),
        coalesce(col("google_region_name"),
                 col("facebook_region_name"),
                 col("website_main_region")).alias("region_name"),
        coalesce(col("google_zip_code"),
                 col("facebook_zip_code")).alias("zip_code"),
        coalesce(col("google_domain"),
                 col("facebook_domain"),
                 col("website_root_domain")).alias("domain"),
        col("facebook_email"),
        current_timestamp().alias("load_timestamp")
    )

    # c = full_df.distinct().count()
    # print(f"Full DF count: {c}")

    # full_df.show()

    full_df.write.csv('./destination', header=True, mode='overwrite')

    spark.stop()


if __name__ == '__main__':
    main()
