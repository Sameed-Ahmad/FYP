-- Sample Database Schema for Testing
-- E-commerce Database

-- Drop existing tables
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Create Categories table
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    description TEXT
);

-- Create Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES categories(category_id),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date DATE DEFAULT CURRENT_DATE
);

-- Create Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending'
);

-- Create Order Items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL
);

-- Insert sample data
INSERT INTO categories (category_name, description) VALUES
('Electronics', 'Electronic devices and gadgets'),
('Clothing', 'Apparel and fashion items'),
('Books', 'Books and publications'),
('Home & Garden', 'Home improvement and garden supplies'),
('Sports', 'Sports equipment and accessories');

INSERT INTO products (product_name, category_id, price, stock_quantity) VALUES
('Laptop Pro 15', 1, 1299.99, 50),
('Wireless Mouse', 1, 29.99, 200),
('USB-C Cable', 1, 12.99, 500),
('Cotton T-Shirt', 2, 19.99, 300),
('Denim Jeans', 2, 59.99, 150),
('Running Shoes', 5, 89.99, 100),
('Python Programming', 3, 45.00, 75),
('Garden Tools Set', 4, 79.99, 40),
('Yoga Mat', 5, 34.99, 120),
('Coffee Maker', 4, 129.99, 60);

INSERT INTO customers (first_name, last_name, email, phone, city, country) VALUES
('John', 'Doe', 'john.doe@email.com', '555-0101', 'New York', 'USA'),
('Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'London', 'UK'),
('Mike', 'Johnson', 'mike.j@email.com', '555-0103', 'Toronto', 'Canada'),
('Sarah', 'Williams', 'sarah.w@email.com', '555-0104', 'Sydney', 'Australia'),
('David', 'Brown', 'david.b@email.com', '555-0105', 'Berlin', 'Germany'),
('Emma', 'Davis', 'emma.d@email.com', '555-0106', 'Paris', 'France'),
('Chris', 'Wilson', 'chris.w@email.com', '555-0107', 'Tokyo', 'Japan'),
('Lisa', 'Martinez', 'lisa.m@email.com', '555-0108', 'Madrid', 'Spain');

INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
(1, '2024-10-01 10:30:00', 1329.98, 'completed'),
(2, '2024-10-02 14:15:00', 89.99, 'completed'),
(3, '2024-10-03 09:45:00', 179.97, 'shipped'),
(1, '2024-10-04 16:20:00', 45.00, 'completed'),
(4, '2024-10-05 11:00:00', 209.98, 'pending'),
(5, '2024-10-06 13:30:00', 129.99, 'shipped'),
(6, '2024-10-07 15:45:00', 59.99, 'completed'),
(7, '2024-10-08 10:15:00', 149.98, 'pending');

INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
(1, 1, 1, 1299.99, 1299.99),
(1, 2, 1, 29.99, 29.99),
(2, 6, 1, 89.99, 89.99),
(3, 4, 3, 19.99, 59.97),
(3, 9, 2, 34.99, 69.98),
(3, 3, 4, 12.99, 51.96),
(4, 7, 1, 45.00, 45.00),
(5, 10, 1, 129.99, 129.99),
(5, 8, 1, 79.99, 79.99),
(6, 10, 1, 129.99, 129.99),
(7, 5, 1, 59.99, 59.99),
(8, 6, 1, 89.99, 89.99),
(8, 4, 3, 19.99, 59.97);

-- Create some useful indexes
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_orders_date ON orders(order_date);