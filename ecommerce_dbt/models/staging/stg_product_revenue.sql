select
	Country,
	StockCode,
	Description,
	CustomerTier,
	Round(TotalRevenue,2) as TotalRevenue,
	OrderCount,
	Rank as ProductRank
from {{source('ecommerce_data','product_revenue')}}
