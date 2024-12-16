-- Check for duplicate transaction_id records
SELECT transaction_id, COUNT(*)
FROM sphere_revenue
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Calculate total revenue by venue and date
SELECT DATE(transaction_date), venue, SUM(revenue) AS total_revenue
FROM sphere_revenue
GROUP BY DATE(transaction_date), venue
ORDER BY DATE(transaction_date), venue;

-- Identify inconsistencies like negative revenue values
SELECT *
FROM sphere_revenue
WHERE revenue < 0;

-- Check if revenue matches quantity * price
SELECT *
FROM sphere_revenue
WHERE ABS(revenue - quantity * price) > 0.01;

--Number of null values in important columns.
SELECT
   COUNT(*) - COUNT(transaction_id) as null_transaction_id,
   COUNT(*) - COUNT(transaction_date) as null_transaction_date,
   COUNT(*) - COUNT(venue) as null_venue,
   COUNT(*) - COUNT(event_name) as null_event_name,
   COUNT(*) - COUNT(ticket_type) as null_ticket_type,
   COUNT(*) - COUNT(quantity) as null_quantity,
   COUNT(*) - COUNT(price) as null_price,
   COUNT(*) - COUNT(revenue) as null_revenue,
   COUNT(*) - COUNT(customer_id) as null_customer_id,
   COUNT(*) - COUNT(payment_method) as null_payment_method
FROM sphere_revenue;