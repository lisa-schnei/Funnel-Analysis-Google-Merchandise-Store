----------------------------------------------------
------- MODULE 3 SPRINT 2 : FUNNEL ANALYSIS --------
----------------------------------------------------

-- Author: Lisa Schneider
-- Date: July 2024
-- Tool used: BigQuery

-- EXPLORATORY ANALYSIS

# 270 154 unique user ids of 4 295 584 total rows
SELECT COUNT (DISTINCT user_pseudo_id) AS unique_ids,
FROM `turing_data_analytics.raw_events`;


# time frame for the data is 2020-11-01 to 2021-01-31.
SELECT MIN(event_date) AS min_date,
MAX(event_date) AS max_date
FROM `turing_data_analytics.raw_events`;


# Translating timestamps for one user with multiple events to understand how to order based on the first (MIN)
# Result: lower timestamp = earlier in time
SELECT user_pseudo_id,
event_name,
event_timestamp,
TIMESTAMP_MICROS(event_timestamp)
FROM `turing_data_analytics.raw_events`
WHERE user_pseudo_id = '1000299.7413851356';


# 17 event categories available in the data 
SELECT DISTINCT event_name,
COUNT (*) AS event_count
FROM `turing_data_analytics.raw_events`
GROUP BY 1
ORDER BY 2 DESC;

# Creating table with unique user_pseudo_ids only
# This result shows that different events can have the same timestamp (e.g. session_start, page_view and first_visit)
# After checking with peers, I clarified that duplicates are defined as the same event_name but different event_timestamp. Example is user id 1000299.7413851356 which has page_view at different timestamps.
SELECT
RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num, # Using window function to assign row_num per user_pseudo_id and event_name 
* 
FROM `turing_data_analytics.raw_events`
ORDER BY user_pseudo_id, event_timestamp;


# Checking logic to see the ranks attributed and if it aligns with the intention of the query
SELECT
RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num,
user_pseudo_id,
event_date,
event_timestamp,
event_name
FROM `turing_data_analytics.raw_events`
ORDER BY user_pseudo_id, event_timestamp;


# Building CTE to use as a base table for further analysis
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num
  FROM `turing_data_analytics.raw_events`)

SELECT *
FROM ranked_events
WHERE row_num = 1
ORDER BY user_pseudo_id;


# 109 countries are represented in the data; top 3 countries by number of events: US, India, Canada
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT country,
COUNT(*) country_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# 3 categories - desktop, mobile, tablet. Most events on desktop, fewest tablet. 
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT category,
COUNT(*) category_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# 5 traffic sources: google, other, direct, shop.googlemerchandisestore.com, data deleted
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT traffic_source,
COUNT(*) AS source_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# 468 unique pages viewed
# data seems to be coming from the Google Merchandise Store
# 'Page unavailable' is ranked high with 18.981 page views
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT page_title,
COUNT(*) AS page_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;



# Events table after eliminiating duplicated events
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT event_name,
COUNT (*) AS event_count
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;

# Splitting the events table by the 3 top countries to understand event distribution per country and event type

WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT event_name,
COUNT(*) AS total,
SUM(CASE WHEN country = 'United States' THEN 1 ELSE 0 END) AS us,
SUM(CASE WHEN country = 'India' THEN 1 ELSE 0 END) AS india,
SUM(CASE WHEN country = 'Canada' THEN 1 ELSE 0 END) AS canada
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# Investigating deeper into session_start event to see how many users do not have a session_start event - 3038 users. This can happen when the same users returns to the page within 30 minutes. After 30 minutes of inactivity, a new session_start event is recorded. 

WITH ranked_events AS (
  SELECT *,
  RANK() OVER (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

-- Subquery to identify user_pseudo_ids with session_start event
, session_start_users AS (
  SELECT DISTINCT user_pseudo_id
  FROM ranked_events
  WHERE row_num = 1
    AND event_name = 'session_start'
)

-- Main query to count user_pseudo_ids without session_start event
SELECT *
FROM ranked_events
WHERE row_num = 1
  AND user_pseudo_id NOT IN (SELECT user_pseudo_id FROM session_start_users)
ORDER BY user_pseudo_id, event_timestamp;



# Building final table for further funnel analysis
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
),

funnel_table AS (
  SELECT
  CASE 
    WHEN event_name = 'session_start' THEN 1
    WHEN event_name = 'view_item' THEN 2
    WHEN event_name = 'add_to_cart' THEN 3
    WHEN event_name = 'begin_checkout' THEN 4
    WHEN event_name = 'add_payment_info' THEN 5
    WHEN event_name = 'purchase' THEN 6
    ELSE NULL
  END AS event_order,
  event_name,
  SUM(CASE WHEN country = 'United States' THEN 1 ELSE 0 END) AS us_events,
  SUM(CASE WHEN country = 'India' THEN 1 ELSE 0 END) AS india_events,
  SUM(CASE WHEN country = 'Canada' THEN 1 ELSE 0 END) AS canada_events
  FROM ranked_events
  WHERE row_num = 1
  GROUP BY 2
)

SELECT *
FROM funnel_table
WHERE event_order BETWEEN 1 AND 6
ORDER BY event_order;


# Building second funnel table to compare device types

WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
  WHERE country IN ('United States', 'India', 'Canada')
),

funnel_table AS (
  SELECT
  CASE 
    WHEN event_name = 'session_start' THEN 1
    WHEN event_name = 'view_item' THEN 2
    WHEN event_name = 'add_to_cart' THEN 3
    WHEN event_name = 'begin_checkout' THEN 4
    WHEN event_name = 'add_payment_info' THEN 5
    WHEN event_name = 'purchase' THEN 6
    ELSE NULL
  END AS event_order,
  event_name,
  SUM(CASE WHEN category = 'desktop' THEN 1 ELSE 0 END) AS desktop_events,
  SUM(CASE WHEN category = 'mobile' THEN 1 ELSE 0 END) AS mobile_events,
  SUM(CASE WHEN category = 'tablet' THEN 1 ELSE 0 END) AS tablet_events
  FROM ranked_events
  WHERE row_num = 1
  GROUP BY 2
)

SELECT *
FROM funnel_table
WHERE event_order BETWEEN 1 AND 6
ORDER BY event_order


