WITH vendas as (
SELECT 
	pro.PRODUCT_NAME
	,COUNT(*) AS TOTAL
FROM public.products as pro
INNER JOIN public.order_details AS ord
ON pro.product_id = ord.product_id
GROUP BY pro.PRODUCT_NAME

)

SELECT 
	DISTINCT(PRODUCT_NAME)
	,TOTAL
FROM vendas
ORDER BY 2 DESC