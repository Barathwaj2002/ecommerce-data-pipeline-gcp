with product as (
    select * from {{ref('stg_product_revenue')}}
    where ProductRank = 1
),
customers as (
    select
        Country,
        count(distinct CustomerId) as CustomerCount,
        round(avg(Monetary), 2) as AvgCustomerSpend,
        round(avg(Frequency), 2) as AvgOrderFrequency
    from {{ref('stg_customer_rfm')}}
    group by Country
)
select
    {{ dbt_utils.generate_surrogate_key(['p.Country','p.Description'])}} as row_id,
    p.Country,
    p.Description AS TopProduct,
    p.TotalRevenue,
    c.CustomerCount,
    c.AvgCustomerSpend,
    c.AvgOrderFrequency
from product p
left join customers c on p.Country=c.Country
order by p.TotalRevenue desc
