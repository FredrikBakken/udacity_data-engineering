# SQL query statements
drop_table = "DROP TABLE IF EXISTS {};"
count_select_maxmind = "SELECT count(*) FROM {};"
count_select_iot23 = "SELECT count(*) FROM {} WHERE insert_date = '{}';"

create_table_asn = """
CREATE TABLE IF NOT EXISTS asn (
    network_id                      VARCHAR NOT NULL UNIQUE,
    network                         VARCHAR,
    autonomous_system_number        INTEGER,
    autonomous_system_organization  VARCHAR,
    PRIMARY KEY (network_id)
);
"""

create_table_city_locations = """
CREATE TABLE IF NOT EXISTS city_locations (
    geoname_id                      INTEGER NOT NULL UNIQUE,
    locale_code                     VARCHAR,
    continent_code                  VARCHAR,
    continent_name                  VARCHAR,
    country_iso_code                VARCHAR,
    country_name                    VARCHAR,
    subdivision_1_iso_code          VARCHAR,
    subdivision_1_name              VARCHAR,
    subdivision_2_iso_code          VARCHAR,
    subdivision_2_name              VARCHAR,
    city_name                       VARCHAR,
    metro_code                      VARCHAR,
    time_zone                       VARCHAR,
    is_in_european_union            INTEGER,
    PRIMARY KEY (geoname_id)
);
"""

create_table_city_blocks = """
CREATE TABLE IF NOT EXISTS city_blocks (
    network_id                      VARCHAR NOT NULL UNIQUE,
    network                         VARCHAR,
    geoname_id                      INTEGER,
    registered_country_geoname_id   INTEGER,
    represented_country_geoname_id  INTEGER,
    is_anonymous_proxy              INTEGER,
    is_satellite_provider           INTEGER,
    postal_code                     VARCHAR,
    latitude                        FLOAT,
    longitude                       FLOAT,
    accuracy_radius                 INTEGER,
    PRIMARY KEY (network_id)
);
"""

create_table_originate_packets = """
CREATE TABLE IF NOT EXISTS originate_packets (
    uid                             VARCHAR NOT NULL UNIQUE,
    host                            VARCHAR,
    port                            INTEGER,
    bytes                           VARCHAR,
    local                           VARCHAR,
    packets                         INTEGER,
    ip_bytes                        INTEGER,
    insert_date                     VARCHAR NOT NULL,
    PRIMARY KEY (uid)
);
"""

create_table_response_packets = """
CREATE TABLE IF NOT EXISTS response_packets (
    uid                             VARCHAR NOT NULL UNIQUE,
    host                            VARCHAR,
    port                            INTEGER,
    bytes                           VARCHAR,
    local                           VARCHAR,
    packets                         INTEGER,
    ip_bytes                        INTEGER,
    insert_date                     VARCHAR NOT NULL,
    PRIMARY KEY (uid)
);
"""

create_table_packets = """
CREATE TABLE IF NOT EXISTS packets (
    timestamp                       VARCHAR,
    uid                             VARCHAR NOT NULL UNIQUE,
    originate_network_id            VARCHAR,
    response_network_id             VARCHAR,
    protocol                        VARCHAR,
    service                         VARCHAR,
    duration                        VARCHAR,
    connection_state                VARCHAR,
    missed_bytes                    INTEGER,
    history                         VARCHAR,
    tunnel_parents                  VARCHAR,
    label                           VARCHAR,
    detailed_label                  VARCHAR,
    insert_date                     VARCHAR NOT NULL,
    PRIMARY KEY (uid)
);
"""
