SELECT *
FROM {{ ref('stg_product_revenue') }}
WHERE TotalRevenue < 0