WITH produtos AS (

SELECT pro.product_name,
	   ord.unit_price,
	   ord.quantity,
	   ord.discount,
	   ord.unit_price * ord.quantity AS total
FROM public.products AS pro
LEFT JOIN public.order_details AS ord
ON pro.product_id = ord.product_id

)

SELECT product_name,
	   sum(total) AS total
FROM produtos
GROUP BY product_name
ORDER BY total DESC