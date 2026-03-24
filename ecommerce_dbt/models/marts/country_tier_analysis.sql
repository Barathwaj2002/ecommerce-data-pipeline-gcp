SELECT
    p.Country,
    p.TopProduct,
    p.TotalRevenue,
    t.Tier,
    p.CustomerCount,
    t.Region,
    p.AvgCustomerSpend
FROM {{ ref('country_performance')}} p LEFT JOIN
{{ ref('customer_tier_mapping')}} t ON 
    p.Country=t.Country
ORDER BY p.TotalRevenue DESC