from os.path import join

# Fixed variables shared over multiple tasks
path_raw_dataset = "/usr/local/airflow/datasets/opt"
path_cleaned_dataset = "/usr/local/airflow/datasets/iot-23"

# Relative paths to dataset directories
path_asn_dataset = "/usr/local/airflow/datasets/GeoLite2-ASN-CSV_20200519"
path_city_dataset = "/usr/local/airflow/datasets/GeoLite2-City-CSV_20200519"

# Paths to the dataset files
path_asn_blocks = join(path_asn_dataset, "GeoLite2-ASN-Blocks-IPv4.csv")
path_city_blocks = join(path_city_dataset, "GeoLite2-City-Blocks-IPv4.csv")
path_city_locations = join(path_city_dataset, "GeoLite2-City-Locations-en.csv")
