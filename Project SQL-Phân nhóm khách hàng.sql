--Tổng quan dữ liệu

Select * 
from transactions
order by 1,2

-----------------Phân tích tỷ lệ rời bỏ của khách hàng-------------------------------

with Customer_first_month as (
	select customer_id
		, MIN (transaction_date) over (PARTITION BY customer_id) as First_time
		, datediff (MONTH, MIN (transaction_date) over (PARTITION BY customer_id), transaction_date) as Retention_month
	from transactions
) 
, Retained as (
	select 
		month (First_time) as First_month
		, Retention_month
		, count (distinct customer_id) as retained_Customer
	from Customer_first_month
	group by month (First_time), Retention_month
)
, Rate as (
	select *
		, FIRST_VALUE (retained_Customer) OVER (PARTITION BY First_month ORDER BY  Retention_month) AS Orginal_Customer
		, format((retained_Customer*1.0)/(FIRST_VALUE (Retained_Customer) OVER (PARTITION BY First_month ORDER BY  Retention_month)), 'p') as Rate
	from Retained
)
select * into RetainedRate from rate

-----pivot table-----
Select First_month, Orginal_Customer
	,"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
from ( 
	select First_month, Retention_month, Orginal_Customer, Rate from RetainedRate 
	) as source_table 
PIVOT (
	  min (Rate)
	  FOR Retention_month IN ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11")
	) 
	PivotTable
order by First_month



----------------------PHÂN NHÓM KHÁCH HÀNG------------------------------------------

with Customer_Behavior as (
select
	customer_id
	, max (transaction_date) as Last_Order_Date
	, DATEDIFF(day, max (transaction_date), '2017-12-30') as Recency
	, count (distinct transaction_id) as Frequency
	, round(sum (list_price),2) as Monetary
from transactions 
group by customer_id 
)
, rfm_percent_rank as (
	select *
	, percent_rank () over (order by Recency asc) as Recency_percent_rank
	, percent_rank () over (order by Frequency desc) as frequency_percent_rank
	, percent_rank () over (order by Monetary desc) as Monetary_percent_rank
	from Customer_Behavior
)
, rfm_rank as (
select * 
	, CASE
		when Recency_percent_rank > 0.75 then 4
		when Recency_percent_rank > 0.5 then 3
		when Recency_percent_rank > 0.25 then 2
		Else 1
	end
	as Recency_Rank
	, CASE
		when frequency_percent_rank > 0.75 then 4
		when frequency_percent_rank > 0.5 then 3
		when frequency_percent_rank > 0.25 then 2
		Else 1
	end
	as Frequency_Rank
	, CASE
		when Monetary_percent_rank > 0.75 then 4
		when Monetary_percent_rank > 0.5 then 3
		when Monetary_percent_rank > 0.25 then 2
		Else 1
	end
	as Monetary_Rank
from rfm_percent_rank
)
, concat_rank as (
	Select customer_id
		, Recency
		, Frequency
		, Monetary
		, Recency_Rank
		, Frequency_Rank
		, Monetary_Rank
		, concat (Recency_Rank, Frequency_Rank, Monetary_Rank) as rfm_rank
	from rfm_rank
)
, rfm_segment as (
 SELECT * 
        , CASE 
        When rfm_rank  =  111 THEN 'VIP Customers'
        When rfm_rank like '[1-2][2-3][1-2]' THEN 'Potential Loyalists' -- KH trung thanh tiem nang
        When rfm_rank like '[1-2]4[1-2]' THEN 'New Customers'
        When rfm_rank like '[3-4][1-2][1-2]' THEN 'Almost Lost' -- sắp lost những KH này 
		When rfm_rank like '[3-4][1-4][1-4]' THEN 'Lost Customers' -- KH cũng rời bỏ nhưng có valued (F = 2)
		When rfm_rank like '[1-2][1-4][1-4]' THEN 'Potential Customers'
        Else 'unknown'
        END 
		as rfm_segment_rank
    FROM concat_rank
)
select * into RFM from rfm_segment
select * from RFM


---Đánh gía chung các nhóm khach hang
select rfm_segment_rank
	, count (customer_id) as Number_customer 
	, format((count (customer_id)*1.0)/sum (count (customer_id)) OVER (), 'p') as Rate_NumberCustomer
	, sum (Monetary) as toltalrevenue
	, format((sum (Monetary)*1.0)/sum (sum (Monetary)) OVER (), 'p') as Rate_Revenue
from RFM
group by rfm_segment_rank
Order by Number_customer asc

