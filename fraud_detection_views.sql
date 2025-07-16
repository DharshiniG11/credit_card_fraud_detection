CREATE DATABASE IF NOT EXISTS fraud_detection;
USE fraud_detection;

CREATE TABLE IF NOT EXISTS transactions (
    txn_id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    txn_time DATETIME,
    location VARCHAR(50),
    device_id VARCHAR(50),
    merchant_category VARCHAR(50),
    is_fraud TINYINT
);

LOAD DATA LOCAL INFILE 'C:/Users/dharshini g/Downloads/credit_card_fraud_dataset.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;
USE fraud_detection;
SELECT COUNT(*) FROM transactions;
SELECT * FROM transactions LIMIT 20;



#  Detect users with rapid, suspicious transactions involving device or location changes.
CREATE OR REPLACE VIEW suspicious_users_view AS
WITH txn_with_gaps AS (
  SELECT user_id, txn_time, amount, location, device_id,
    LAG(txn_time) OVER (PARTITION BY user_id ORDER BY txn_time) AS prev_txn_time,
    LAG(location) OVER (PARTITION BY user_id ORDER BY txn_time) AS prev_location,
    LAG(device_id) OVER (PARTITION BY user_id ORDER BY txn_time) AS prev_device
  FROM transactions
),
flagged_bursts AS (
  SELECT *,
    TIMESTAMPDIFF(MINUTE, prev_txn_time, txn_time) AS time_gap,
    CASE 
      WHEN prev_txn_time IS NOT NULL 
           AND TIMESTAMPDIFF(MINUTE, prev_txn_time, txn_time) <= 5
           AND (location != prev_location OR device_id != prev_device)
      THEN 1 ELSE 0
    END AS suspicious_flag
  FROM txn_with_gaps
),
suspicious_users AS (
  SELECT  user_id,
    COUNT(*) AS flagged_count,
    SUM(amount) AS total_flagged_amount,
    MIN(txn_time) AS first_flagged_txn,
    MAX(txn_time) AS last_flagged_txn
  FROM flagged_bursts
  WHERE suspicious_flag = 1
  GROUP BY user_id
  HAVING flagged_count >= 2 AND total_flagged_amount > 3000
)
SELECT * 
FROM suspicious_users
ORDER BY total_flagged_amount DESC;
SELECT * FROM suspicious_users_view;

# 2 Identify devices shared by multiple users (>= 3)
SELECT device_id, COUNT(DISTINCT user_id) AS user_count
FROM transactions
GROUP BY device_id
HAVING user_count >= 3;

# Devices used by multiple users
CREATE OR REPLACE VIEW suspicious_shared_device_view AS
WITH shared_devices AS (
  SELECT device_id
  FROM transactions
  GROUP BY device_id
  HAVING COUNT(DISTINCT user_id) >= 3
),
# Transactions from those devices during odd hours or high amounts
suspicious_txns AS (
  SELECT *
  FROM transactions
  WHERE (HOUR(txn_time) BETWEEN 1 AND 5 OR amount > 5000)
)

SELECT s.user_id, s.txn_time, s.device_id, s.amount, s.location
FROM suspicious_txns s
JOIN shared_devices d ON s.device_id = d.device_id
ORDER BY s.device_id, s.txn_time;
SELECT * FROM suspicious_shared_device_view;


-- View to get frequency of first digit in transaction amounts
-- Enhanced view to include first digit frequency, actual %, and expected Benford's %
CREATE OR REPLACE VIEW benfords_law_view AS
WITH digit_counts AS (
  SELECT
    LEFT(CAST(amount AS CHAR), 1) AS first_digit,
    COUNT(*) AS frequency
  FROM transactions
  WHERE amount >= 1
  GROUP BY first_digit
),
total_count AS (
  SELECT COUNT(*) AS total_txns
  FROM transactions
  WHERE amount >= 1
)
SELECT 
  d.first_digit,
  d.frequency,
  ROUND(d.frequency / t.total_txns * 100, 2) AS actual_percentage,
  CASE d.first_digit
    WHEN '1' THEN 30.1
    WHEN '2' THEN 17.6
    WHEN '3' THEN 12.5
    WHEN '4' THEN 9.7
    WHEN '5' THEN 7.9
    WHEN '6' THEN 6.7
    WHEN '7' THEN 5.8
    WHEN '8' THEN 5.1
    WHEN '9' THEN 4.6
    ELSE NULL
  END AS expected_benford_percentage
FROM digit_counts d, total_count t
ORDER BY d.first_digit;
select * from benfords_law_view;



CREATE OR REPLACE VIEW user_weekday_weekend_comparison AS
-- Left join: all users with weekday txns + matching weekend txns
SELECT 
  w.user_id,
  w.weekday_count,
  COALESCE(we.weekend_count, 0) AS weekend_count,
  ROUND(COALESCE(we.weekend_count, 0) / NULLIF(w.weekday_count, 0), 2) AS weekend_to_weekday_ratio
FROM (
  SELECT user_id, COUNT(*) AS weekday_count
  FROM transactions
  WHERE DAYOFWEEK(txn_time) BETWEEN 2 AND 6
  GROUP BY user_id
) w
LEFT JOIN (
  SELECT user_id, COUNT(*) AS weekend_count
  FROM transactions
  WHERE DAYOFWEEK(txn_time) IN (1, 7)
  GROUP BY user_id
) we ON w.user_id = we.user_id

UNION

-- Right join simulation: all users with weekend txns + unmatched weekday txns
SELECT 
  we.user_id,
  COALESCE(w.weekday_count, 0) AS weekday_count,
  we.weekend_count,
  ROUND(we.weekend_count / NULLIF(COALESCE(w.weekday_count, 0), 0), 2) AS weekend_to_weekday_ratio
FROM (
  SELECT user_id, COUNT(*) AS weekend_count
  FROM transactions
  WHERE DAYOFWEEK(txn_time) IN (1, 7)
  GROUP BY user_id
) we
LEFT JOIN (
  SELECT user_id, COUNT(*) AS weekday_count
  FROM transactions
  WHERE DAYOFWEEK(txn_time) BETWEEN 2 AND 6
  GROUP BY user_id
) w ON we.user_id = w.user_id
WHERE w.user_id IS NULL;
select * from user_weekday_weekend_comparison;

CREATE OR REPLACE VIEW suspicious_small_txn_bursts_view AS
SELECT
  user_id,
  DATE(txn_time) AS txn_date,
  COUNT(*) AS small_txn_count,
  SUM(amount) AS total_amount,
  MIN(txn_time) AS first_txn_time,
  MAX(txn_time) AS last_txn_time
FROM transactions
WHERE amount BETWEEN 4000 AND 4999
GROUP BY user_id, DATE(txn_time)
HAVING small_txn_count >= 2 AND total_amount > 8000;
select * from suspicious_small_txn_bursts_view;

CREATE OR REPLACE VIEW suspicious_dormant_accounts_view AS
WITH txn_gaps AS (
  SELECT 
    user_id,
    txn_time,
    amount,
    LAG(txn_time) OVER (PARTITION BY user_id ORDER BY txn_time) AS prev_txn_time,
    DATEDIFF(txn_time, LAG(txn_time) OVER (PARTITION BY user_id ORDER BY txn_time)) AS gap_days
  FROM transactions
),
dormant_followed_by_burst AS (
  SELECT 
    user_id,
    DATE(txn_time) AS txn_date,
    COUNT(*) AS txn_count_after_gap,
    MIN(txn_time) AS first_txn_time,
    MAX(txn_time) AS last_txn_time
  FROM txn_gaps
  WHERE gap_days >= 30
  GROUP BY user_id, DATE(txn_time)
  HAVING txn_count_after_gap >= 2
)
SELECT * FROM dormant_followed_by_burst
ORDER BY txn_count_after_gap DESC;

select * from suspicious_dormant_accounts_view;
