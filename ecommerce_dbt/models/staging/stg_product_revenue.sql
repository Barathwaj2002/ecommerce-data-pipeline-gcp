select
	{{ clean_string('Country') }} as Country,
	StockCode,
	{{ clean_string('Description') }} as Description,
	CustomerTier,
	Round(TotalRevenue,2) as TotalRevenue,
	OrderCount,
	Rank as ProductRank
from {{source('ecommerce_data','product_revenue')}}
