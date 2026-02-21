{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'generated']
    )
}}

WITH date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2031-01-01' as date)"
        )
    }}
),

date_dimension AS (
    SELECT
        CAST(REPLACE(CAST((date_day::TIMESTAMP::DATE) AS VARCHAR), '-', '') AS INTEGER) AS date_key,
        
        date_day AS date_actual,
        
        YEAR(date_day) AS year,
        'Y' || CAST(YEAR(date_day) AS VARCHAR) AS year_name,
        
        QUARTER(date_day) AS quarter,
        'Q' || CAST(QUARTER(date_day) AS VARCHAR) AS quarter_name,
        'Y' || CAST(YEAR(date_day) AS VARCHAR) || '-Q' || CAST(QUARTER(date_day) AS VARCHAR) AS year_quarter,
        
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,
        LEFT(MONTHNAME(date_day), 3) AS month_name_short,
        'Y' || CAST(YEAR(date_day) AS VARCHAR) || '-M' || LPAD(CAST(MONTH(date_day) AS VARCHAR), 2, '0') AS year_month,
        
        WEEKOFYEAR(date_day) AS week_of_year,
        'W' || LPAD(CAST(WEEKOFYEAR(date_day) AS VARCHAR), 2, '0') AS week_name,
        'Y' || CAST(YEAR(date_day) AS VARCHAR) || '-W' || LPAD(CAST(WEEKOFYEAR(date_day) AS VARCHAR), 2, '0') AS year_week,
        
        DAY(date_day) AS day_of_month,
        DAYOFYEAR(date_day) AS day_of_year,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        LEFT(DAYNAME(date_day), 3) AS day_name_short,
        
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
        
        CASE 
            WHEN MONTH(date_day) = 1 AND DAY(date_day) = 1 THEN 'New Year''s Day'
            WHEN MONTH(date_day) = 7 AND DAY(date_day) = 4 THEN 'Independence Day'
            WHEN MONTH(date_day) = 12 AND DAY(date_day) = 25 THEN 'Christmas Day'
            WHEN MONTH(date_day) = 11 AND DAY(date_day) >= 22 AND DAY(date_day) <= 28 
                 AND DAYOFWEEK(date_day) = 4 THEN 'Thanksgiving'
            ELSE NULL
        END AS holiday_name,
        
        CASE 
            WHEN MONTH(date_day) = 1 AND DAY(date_day) = 1 THEN TRUE
            WHEN MONTH(date_day) = 7 AND DAY(date_day) = 4 THEN TRUE
            WHEN MONTH(date_day) = 12 AND DAY(date_day) = 25 THEN TRUE
            WHEN MONTH(date_day) = 11 AND DAY(date_day) >= 22 AND DAY(date_day) <= 28 
                 AND DAYOFWEEK(date_day) = 4 THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        
        YEAR(date_day) AS fiscal_year,
        QUARTER(date_day) AS fiscal_quarter,
        MONTH(date_day) AS fiscal_month,
        
        CASE WHEN date_day = CURRENT_DATE THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE - INTERVAL 1 DAY THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN date_day = CURRENT_DATE + INTERVAL 1 DAY THEN TRUE ELSE FALSE END AS is_tomorrow,
        
        CASE 
            WHEN date_day >= DATE_TRUNC('week', CURRENT_DATE) 
                AND date_day < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL 7 DAY 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_week,
        
        CASE 
            WHEN date_day >= DATE_TRUNC('month', CURRENT_DATE) 
                AND date_day < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL 1 MONTH 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_month,
        
        CASE 
            WHEN date_day >= DATE_TRUNC('quarter', CURRENT_DATE) 
                AND date_day < DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL 3 MONTH 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_quarter,
        
        CASE 
            WHEN date_day >= DATE_TRUNC('year', CURRENT_DATE) 
                AND date_day < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL 1 YEAR 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_year,
        
        CASE WHEN date_day >= CURRENT_DATE - INTERVAL 7 DAY AND date_day <= CURRENT_DATE THEN TRUE ELSE FALSE END AS is_last_7_days,
        CASE WHEN date_day >= CURRENT_DATE - INTERVAL 30 DAY AND date_day <= CURRENT_DATE THEN TRUE ELSE FALSE END AS is_last_30_days,
        CASE WHEN date_day >= CURRENT_DATE - INTERVAL 90 DAY AND date_day <= CURRENT_DATE THEN TRUE ELSE FALSE END AS is_last_90_days,
        CASE WHEN date_day >= CURRENT_DATE - INTERVAL 365 DAY AND date_day <= CURRENT_DATE THEN TRUE ELSE FALSE END AS is_last_365_days,
        
        CASE WHEN DAY(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
        CASE WHEN date_day = LAST_DAY(date_day) THEN TRUE ELSE FALSE END AS is_last_day_of_month,
        
        CURRENT_TIMESTAMP AS created_at
        
    FROM date_spine
)

SELECT * FROM date_dimension
ORDER BY date_actual