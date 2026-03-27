SELECT *
FROM {{ ref('stg_product_revenue') }}
WHERE ProductRank > 5