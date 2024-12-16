CREATE TABLE IF NOT EXISTS sphere_revenue (
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date TIMESTAMP,
    venue VARCHAR(255),
    event_name VARCHAR(255),
    ticket_type VARCHAR(255),
    quantity INT,
    price DECIMAL(10,2),
    revenue DECIMAL(10,2),
    customer_id VARCHAR(255),
    payment_method VARCHAR(255)
);