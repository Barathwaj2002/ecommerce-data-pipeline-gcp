{% snapshot customer_rfm_snapshot %}
{{
    config(
        target_schema='ecommerce_dbt_snapshots',
        unique_key='CustomerId',
        strategy='check',
        check_cols=['Monetary', 'Frequency']
    )
}}
SELECT
    CustomerId,
    Country,
    Monetary,
    Frequency,
    LastPurchaseRecency
FROM {{ source('ecommerce_data','customer_rfm')}}
{% endsnapshot %}