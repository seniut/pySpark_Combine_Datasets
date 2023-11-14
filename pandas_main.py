import pandas as pd
import hashlib


def add_column_prefix(df, prefix, except_columns=('hash_key',)):
    df.columns = [prefix + '_' + column if column not in except_columns else column for column in df.columns]
    return df


def key_cleaner(column_value):
    return column_value.lower().strip().replace(r'\s+|\r|\n', '')


def create_hash_key(row, columns):
    combined_string = "".join([key_cleaner(str(row[column])) for column in columns])
    return hashlib.sha256(combined_string.encode()).hexdigest()


def main():

    # Read CSV files
    facebook_df = pd.read_csv('datasets/facebook_dataset.csv', sep=',', encoding='utf-8', quotechar='"')
    google_df = pd.read_csv('datasets/google_dataset.csv', sep=',', encoding='utf-8', quotechar='"')
    website_df = pd.read_csv('datasets/website_dataset.csv', sep=';', encoding='utf-8', quotechar='"',
                             lineterminator='\n')

    # Rename columns for joining
    facebook_df.rename(
        columns={"name": "company_name", "country_name": "country_name", "city": "city", 'categories': 'category'},
        inplace=True)
    google_df.rename(columns={"name": "company_name", "country_name": "country_name", "city": "city"}, inplace=True)
    website_df.rename(columns={"legal_name": "company_name", "main_country": "country_name", "main_city": "city",
                               's_category': 'category'}, inplace=True)

    # Selecting columns
    facebook_selected_columns = ["category", "address", "country_name", "country_code", "city", "email", "company_name",
                                 "phone", "phone_country_code", "region_name", "zip_code", "domain"]
    google_selected_columns = ["category", "address", "country_name", "country_code", "city", "company_name", "phone",
                               "phone_country_code", "region_name", "zip_code", "domain"]
    website_selected_columns = ["root_domain", "company_name", "country_name", "city", "main_region", "category"]

    facebook_df = facebook_df[facebook_selected_columns]
    google_df = google_df[google_selected_columns]
    website_df = website_df[website_selected_columns]

    # Creating hash key
    hash_columns = ["company_name", "country_name", "city"]
    facebook_df["hash_key"] = facebook_df.apply(lambda row: create_hash_key(row, hash_columns), axis=1)
    google_df["hash_key"] = google_df.apply(lambda row: create_hash_key(row, hash_columns), axis=1)
    website_df["hash_key"] = website_df.apply(lambda row: create_hash_key(row, hash_columns), axis=1)

    facebook_df = add_column_prefix(facebook_df, 'facebook')
    google_df = add_column_prefix(google_df, 'google')
    website_df = add_column_prefix(website_df, 'website')

    # Joining datasets
    full_df = pd.merge(google_df, facebook_df, on='hash_key', how='inner')
    full_df = pd.merge(full_df, website_df, on='hash_key', how='inner')

    # Selecting final columns and coalescing values
    full_df['company_name'] = full_df[['google_company_name', 'facebook_company_name', 'website_company_name']].bfill(axis=1).iloc[:, 0]
    full_df['category'] = full_df[['google_category', 'facebook_category', 'website_category']].apply(lambda x: ' | '.join(x.dropna()), axis=1)
    full_df['address'] = full_df[['google_address', 'facebook_address']].bfill(axis=1).iloc[:, 0]
    full_df['country_name'] = full_df[['google_country_name', 'facebook_country_name', 'website_country_name']].bfill(axis=1).iloc[:, 0]
    full_df['country_code'] = full_df[['google_country_code', 'facebook_country_code']].bfill(axis=1).iloc[:, 0]
    full_df['city'] = full_df[['google_city', 'facebook_city', 'website_city']].bfill(axis=1).iloc[:, 0]
    full_df['phone'] = full_df[['google_phone', 'facebook_phone']].bfill(axis=1).iloc[:, 0]
    full_df['region_name'] = full_df[['google_region_name', 'facebook_region_name', 'website_main_region']].bfill(axis=1).iloc[:, 0]
    full_df['zip_code'] = full_df[['google_zip_code', 'facebook_zip_code']].bfill(axis=1).iloc[:, 0]
    full_df['domain'] = full_df[['google_domain', 'facebook_domain', 'website_root_domain']].bfill(axis=1).iloc[:, 0]
    full_df['email'] = full_df['facebook_email']

    # Add load timestamp
    full_df['load_timestamp'] = pd.Timestamp.now()

    # Write to CSV
    full_df.to_csv('./destination/merged_dataset.csv', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
