SELECT * FROM
stadiums

---- top 5 stadiums by Capacity ---
SELECT TOP 10 rank , stadium , capacity , region , country , city
FROM stadiums
ORDER BY capacity DESC

--- ABG capacity of stadiums by region
SELECT region , avg(capacity) as 'avg_capacity' 
FROM stadiums
GROUP by region
ORDER BY 'avg_capacity' DESC

--- Number of stadiums in each country ----
SELECT country , COUNT(stadium) AS 'number_stadiums'
FROM stadiums
GROUP BY country 
ORDER BY 'number_stadiums' DESC

--- stadium ranking with each region
SELECT rank , region , stadium , capacity , 
    RANK() OVER(PARTITION BY region ORDER BY capacity DESC) AS region_rank
FROM stadiums


--- stadiums with capacity above the avg ---

SELECT * 
FROM stadiums s
WHERE s.capacity > (SELECT  AVG(capacity) FROM stadiums)


--- stadiums with coloses capacity 
WITH MedianCTE AS (
    SELECT DISTINCT region,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity) 
        OVER (PARTITION BY region) AS median_capacity
    FROM stadiums
),
ranked_stadiums AS (
    SELECT 
        s."rank",  -- quote "rank" if it's a reserved keyword
        s.stadium,
        s.region,
        s.capacity,
        ROW_NUMBER() OVER (
            PARTITION BY s.region 
            ORDER BY ABS(s.capacity - m.median_capacity)
        ) AS median_rank
    FROM stadiums s
    JOIN MedianCTE m ON s.region = m.region
)
SELECT "rank", stadium, region, capacity
FROM ranked_stadiums
WHERE median_rank = 1;





