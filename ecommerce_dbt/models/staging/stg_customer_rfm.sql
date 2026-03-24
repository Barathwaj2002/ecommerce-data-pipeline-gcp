SELECT
  CAST(CustomerId as INT64) as CustomerId,
  Country,
  ROUND(Monetary, 2) as Monetary,
  Frequency,
  LastPurchaseRecency
FROM {{ source('ecommerce_data', 'customer_rfm') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY Monetary DESC) = 1