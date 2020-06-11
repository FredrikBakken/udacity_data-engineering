from os.path import join

# Raw and cleaned IoT-23 dataset paths
path_raw_dataset = "/usr/local/airflow/datasets/opt"
path_cleaned_dataset = "/usr/local/airflow/datasets/iot-23"

# Honeypot capture paths
path_captures = join(path_raw_dataset, "Malware-Project/BigDataset/IoTScenarios")
path_honeypot_capture_4_1 = join(path_captures, "CTU-Honeypot-Capture-4-1")
path_honeypot_capture_5_1 = join(path_captures, "CTU-Honeypot-Capture-5-1")
path_honeypot_capture_7_1 = join(path_captures, "CTU-Honeypot-Capture-7-1")

# Relative paths to dataset directories
path_asn_dataset = "/usr/local/airflow/datasets/GeoLite2-ASN-CSV_20200519"
path_city_dataset = "/usr/local/airflow/datasets/GeoLite2-City-CSV_20200519"

# Paths to the dataset files
path_asn_blocks = join(path_asn_dataset, "GeoLite2-ASN-Blocks-IPv4.csv")
path_city_blocks = join(path_city_dataset, "GeoLite2-City-Blocks-IPv4.csv")
path_city_locations = join(path_city_dataset, "GeoLite2-City-Locations-en.csv")

# IoT-23 dataset path
def get_iot23_partition(year, month, day):
    return "/usr/local/airflow/datasets/iot-23/year={}/month={}/day={}".format(year, month, day)
