/*Джерела даних у ./sources/sql/ */

CREATE TABLE dim_products(
 product_id INT PRIMARY KEY,
 item VARCHAR(200),
 category VARCHAR (100)
)

CREATE TYPE customer_type_enum AS ENUM ('private', 'wholesaler');

CREATE TABLE dim_customers (
 customer_id INT PRIMARY KEY,
 customer_type customer_type_enum
);

CREATE TABLE src_purchases (
 invoice_id INT,
 product_id INT,
 invoice_date DATE,
 customer_id INT,
 quantity INT,

 PRIMARY KEY (invoice_id, product_id),

 CONSTRAINT fk_purchases_products
  FOREIGN KEY (product_id)
  REFERENCES dim_products(product_id),

 CONSTRAINT fk_src_purchases_customers
  FOREIGN KEY (customer_id)
  REFERENCES dim_customers(customer_id)
);

CREATE TABLE src_invoices (
 invoice_id INT,
 product_id INT,
 quantity INT,
 line_total NUMERIC(10,2),
 price NUMERIC(10,2)

 PRIMARY KEY (invoice_id, product_id),

 CONSTRAINT fk_invoices_products
  FOREIGN KEY (product_id)
  REFERENCES dim_products(product_id)
);

/* Retention Знайти клієнтів, чия перша покупка була 2014-12, і скільки з них повернулось у наступні місяці*/ 
WITH cte_first_buy AS (
 SELECT 
  *,
  MIN(invoice_date) OVER (PARTITION BY customer_id) AS first_purchase_date
 FROM src_purchases
), # знайти 1 покупку

cte_sample AS (
 SELECT *
 FROM cte_first_buy
 WHERE DATE_TRUNC('month', first_purchase_date) = DATE '2014-12-01'
), # знайти клієнтів у кого 1 покупку, була 2014-12

cte_active_customers AS (
 SELECT
  DATE_TRUNC('month', invoice_date)::date AS invoice_month,
  COUNT(DISTINCT customer_id) AS active_customers
 FROM cte_sample
 GROUP BY DATE_TRUNC('month', invoice_date)::date
) # серед цих клієнтів, порахувати їх активність помісячно
SELECT 
 *,
 ROUND(active_customers::numeric / MAX(active_customers) OVER (), 2) AS retention
FROM cte_active_customers
ORDER BY invoice_month; # вивести скільки % від початкової групи повернулось

/* Anti-join  Знайти клієнтів у кого 20 > тис продажів, але вивести у dim_customers протилежних (<=20 тис.) */

WITH cte_fact AS (
 SELECT 
     p.customer_id, 
     i.price * i.quantity AS total_sum
 FROM src_invoices i
 LEFT JOIN src_purchases p
 USING (invoice_id, product_id)
),

cte_group AS (
 SELECT 
     customer_id, 
     SUM(total_sum) AS total_sum
 FROM cte_fact 
 GROUP BY customer_id
 HAVING SUM(total_sum) > 20000
)

SELECT *
FROM dim_customers ds
WHERE NOT EXISTS (
 SELECT NULL
 FROM cte_group cg
 WHERE cg.customer_id = ds.customer_id
);

/*Топ-N по категорії Вивести ті, продукти які забезпечують 70% продажів*/
WITH cte_fact AS (
 SELECT 
  d.product_id,
  SUM(i.price * i.quantity) AS product_revenue
 FROM src_invoices i
 JOIN src_purchases p 
	USING (invoice_id, product_id)
 JOIN dim_products d 
	ON d.product_id = p.product_id
 GROUP BY d.product_id
),

cte_cum AS (
 SELECT *,
  SUM(product_revenue) OVER (ORDER BY product_revenue DESC) AS cumulative_revenue, #першими їдуть найбільші
  SUM(product_revenue) OVER () AS total_revenue
 FROM cte_fact
)

SELECT product_id
FROM cte_cum
WHERE cumulative_revenue / total_revenue <= 0.70;
