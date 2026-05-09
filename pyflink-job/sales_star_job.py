from __future__ import annotations

import os
from dataclasses import dataclass

from pyflink.table import EnvironmentSettings, StreamTableEnvironment


@dataclass(frozen=True)
class AppConfig:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group: str
    pg_host: str
    pg_port: str
    pg_db: str
    pg_user: str
    pg_password: str


def env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value and value.strip() else default


def load_config() -> AppConfig:
    return AppConfig(
        kafka_bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=env("KAFKA_TOPIC", "sales_topic"),
        kafka_group=env("KAFKA_GROUP_ID", "flink-lab3-star"),
        pg_host=env("POSTGRES_HOST", "postgres"),
        pg_port=env("POSTGRES_PORT", "5432"),
        pg_db=env("POSTGRES_DB", "snowflake_db"),
        pg_user=env("POSTGRES_USER", "user"),
        pg_password=env("POSTGRES_PASSWORD", "password"),
    )


def ddl_source(cfg: AppConfig) -> str:
    return f"""
    CREATE TABLE sales_events (
        id INT,
        customer_first_name STRING,
        customer_last_name STRING,
        customer_age INT,
        customer_email STRING,
        customer_country STRING,
        customer_postal_code STRING,
        customer_pet_type STRING,
        customer_pet_name STRING,
        customer_pet_breed STRING,
        seller_first_name STRING,
        seller_last_name STRING,
        seller_email STRING,
        seller_country STRING,
        seller_postal_code STRING,
        product_name STRING,
        product_category STRING,
        product_price DECIMAL(10,2),
        product_quantity INT,
        sale_date STRING,
        sale_customer_id INT,
        sale_seller_id INT,
        sale_product_id INT,
        sale_quantity INT,
        sale_total_price DECIMAL(12,2),
        store_name STRING,
        store_location STRING,
        store_city STRING,
        store_state STRING,
        store_country STRING,
        store_phone STRING,
        store_email STRING,
        pet_category STRING,
        product_weight DECIMAL(10,2),
        product_color STRING,
        product_size STRING,
        product_brand STRING,
        product_material STRING,
        product_description STRING,
        product_rating DECIMAL(3,1),
        product_reviews INT,
        product_release_date STRING,
        product_expiry_date STRING,
        supplier_name STRING,
        supplier_contact STRING,
        supplier_email STRING,
        supplier_phone STRING,
        supplier_address STRING,
        supplier_city STRING,
        supplier_country STRING,
        event_time AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{cfg.kafka_topic}',
        'properties.bootstrap.servers' = '{cfg.kafka_bootstrap}',
        'properties.group.id' = '{cfg.kafka_group}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """


def jdbc_url(cfg: AppConfig) -> str:
    return f"jdbc:postgresql://{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_db}"


def creds(cfg: AppConfig) -> str:
    return f"'username' = '{cfg.pg_user}', 'password' = '{cfg.pg_password}'"


def ddl_sink(cfg: AppConfig, table_name: str, columns: str, primary_key: str | None = None, batch_rows: int = 5000) -> str:
    key_clause = f", PRIMARY KEY ({primary_key}) NOT ENFORCED" if primary_key else ""
    return f"""
    CREATE TABLE {table_name} ({columns}{key_clause}) WITH (
        'connector' = 'jdbc',
        'url' = '{jdbc_url(cfg)}',
        'table-name' = '{table_name}',
        {creds(cfg)},
        'sink.buffer-flush.max-rows' = '{batch_rows}',
        'sink.buffer-flush.interval' = '1s',
        'sink.max-retries' = '3'
    )
    """


def register_sinks(t_env: StreamTableEnvironment, cfg: AppConfig) -> None:
    ddl_map = {
        "dim_stores": (
            "store_name STRING, store_location STRING, store_city STRING, store_state STRING, "
            "store_country STRING, store_phone STRING, store_email STRING",
            "store_name",
        ),
        "dim_suppliers": (
            "supplier_name STRING, supplier_contact STRING, supplier_email STRING, supplier_phone STRING, "
            "supplier_address STRING, supplier_city STRING, supplier_country STRING",
            "supplier_name",
        ),
        "dim_customers": (
            "customer_id INT, customer_first_name STRING, customer_last_name STRING, customer_email STRING, "
            "customer_age INT, customer_country STRING, customer_postal_code STRING, customer_pet_type STRING, "
            "customer_pet_name STRING, customer_pet_breed STRING",
            "customer_id",
        ),
        "dim_sellers": (
            "seller_id INT, seller_first_name STRING, seller_last_name STRING, seller_email STRING, "
            "seller_country STRING, seller_postal_code STRING",
            "seller_id",
        ),
        "dim_products": (
            "product_id INT, product_name STRING, product_category STRING, product_brand STRING, "
            "product_price DECIMAL(10,2), product_weight DECIMAL(10,2), product_color STRING, product_size STRING, "
            "product_material STRING, product_description STRING, product_rating DECIMAL(3,1), product_reviews INT, "
            "product_release_date STRING, product_expiry_date STRING, pet_category STRING, supplier_id INT",
            "product_id",
        ),
        "fact_sales": (
            "sale_date DATE, customer_id INT, product_id INT, seller_id INT, quantity INT, total_price DECIMAL(12,2)",
            None,
        ),
    }

    for table_name, (columns, primary_key) in ddl_map.items():
        t_env.execute_sql(ddl_sink(cfg, table_name, columns, primary_key))


def register_inserts(t_env: StreamTableEnvironment) -> None:
    statement_set = t_env.create_statement_set()
    statements = [
        """
        INSERT INTO dim_stores
        SELECT store_name, store_location, store_city, store_state, store_country, store_phone, store_email
        FROM sales_events
        """,
        """
        INSERT INTO dim_suppliers
        SELECT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country
        FROM sales_events
        """,
        """
        INSERT INTO dim_customers
        SELECT sale_customer_id, customer_first_name, customer_last_name, customer_email, customer_age,
               customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed
        FROM sales_events
        """,
        """
        INSERT INTO dim_sellers
        SELECT sale_seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
        FROM sales_events
        """,
        """
        INSERT INTO dim_products
        SELECT
            e.sale_product_id,
            e.product_name,
            e.product_category,
            e.product_brand,
            e.product_price,
            e.product_weight,
            e.product_color,
            e.product_size,
            e.product_material,
            e.product_description,
            e.product_rating,
            e.product_reviews,
            e.product_release_date,
            e.product_expiry_date,
            e.pet_category,
            CAST(NULL AS INT) AS supplier_id
        FROM sales_events e
        """,
        """
        INSERT INTO fact_sales
        SELECT TO_DATE(sale_date, 'M/d/yyyy'), sale_customer_id, sale_product_id, sale_seller_id, sale_quantity, sale_total_price
        FROM sales_events
        """,
    ]

    for sql in statements:
        statement_set.add_insert_sql(sql)

    statement_set.execute()


def main() -> None:
    cfg = load_config()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    t_env.execute_sql(ddl_source(cfg))
    register_sinks(t_env, cfg)

    print("Flink job submitted.")
    register_inserts(t_env)


if __name__ == "__main__":
    main()
