--create table customers
CREATE TABLE flipcart.customers
(
customer_id int,
customer_name varchar(35),
state varchar(25)
);

select * from flipcart.customers;

create table flipcart.products
(
product_id int,
product_name varchar(45),
price numeric(10,3),
cogs numeric(10,3),
category varchar(25),
brand varchar(25)
);

select * from flipcart.products;

alter table flipcart.customers add constraint customer_pk primary key(customer_id);

alter table flipcart.products add constraint products_pk primary key(product_id);

create table flipcart.sales
(
order_id int primary key,
order_date date,
customer_id int,
order_status varchar(25),
product_id int,
quantity int,
price_per_unit numeric(10,3),
foreign key(customer_id) references flipcart.customers(customer_id),
foreign key(product_id) references flipcart.products(product_id)
);

select * from flipcart.sales;

create table flipcart.shippings
(
shipping_id int primary key,
order_id int,
shipping_date date,
return_date date,
shipping_products varchar(35),
delivery_satus varchar(35),
foreign key(order_id) references flipcart.sales(order_id)
);

select * from flipcart.shippings;

create table flipcart.payments
(
payment_id int primary key,
order_id int,
payment_date date,
payment_status varchar(35),
foreign key(order_id) references flipcart.sales(order_id)
);

select * from flipcart.payments;