{% docs table_report %}

This table contains the computed metrics for the order data, by purchase date grain
The data in the orders table is filtered by the order status not equal to 'canceled'

- Total orders: Count of distinct order ids in the Order table, grouped by the purchase date
- Total customers making orders: Count of distinct customer ids in the Order table, grouped by the purchase date
- Total revenue: Sum of the payment value in the Order-Payments table, grouped by the purchase date
- Average revenue per order: Total revenue divided by the total orders
- Top 3 categories by revenue: 
    - Data from the orders, items and products tables are is joined by the order id.
    - The sum of the product price and the freight value is sumed up, and then accumulated by the purchase date and the product category.
    - Product categories are ranked iin descendant order by the previous computation.
    - Data is filtered to keep only the top 3 categories by purchase date.
- Percent of day's revenue associated with each of the top 3 product categories: The sum of the revenue for each product category is divided by the total revenue of the day.

{% enddocs %}