-- a. Total number of cars by model by country
SELECT model, country, SUM(sales_volume) AS total_sales
FROM consumers
GROUP BY model, country
ORDER BY total_sales DESC;

-- b. Country with the most of each model
SELECT model, country, MAX(sales_volume) AS max_sales
FROM consumers
GROUP BY model, country
ORDER BY max_sales DESC;

-- c. Models sold in USA but not in France
SELECT model
FROM consumers
WHERE country = 'USA'
AND model NOT IN (SELECT model FROM consumers WHERE country = 'France');

-- d. Average car price per country by engine type
SELECT c.country, car.engine_type, AVG(car.price) AS avg_price
FROM cars car
JOIN consumers c ON car.model = c.model
GROUP BY c.country, car.engine_type
ORDER BY avg_price DESC;

-- e. Average ratings of electric vs thermal cars
SELECT car.engine_type, AVG(c.review_score) AS avg_rating
FROM cars car
JOIN consumers c ON car.model = c.model
GROUP BY car.engine_type
ORDER BY avg_rating DESC;
