# Seeds — Olist E-Commerce Dataset

dbt seeds load CSV files directly into Snowflake as tables.
The Olist CSV files are not committed to this repo (too large for git).

## How to get the data

1. Go to https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Click **Download** (requires free Kaggle account)
3. Unzip and place these 8 CSV files in this `seeds/` folder:

```
seeds/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
└── product_category_name_translation.csv
```

4. Run `dbt seed` — this loads all CSVs into `ECOMMERCE_RAW.OLIST.*`

## Why seeds for source data?

For this portfolio project, seeds are the simplest way to get data into
Snowflake without setting up an ingestion pipeline. In production, source
data would land via an ingestion tool (Fivetran, Airbyte, ADF, or a
custom pipeline like Project 1).
