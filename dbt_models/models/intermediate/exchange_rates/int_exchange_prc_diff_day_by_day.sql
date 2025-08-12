WITH wk_exchange_prc_diff_day_by_day AS (

    SELECT 
        prev_date.DATE_VAL as prev_date, 
        date.DATE_VAL,
        prev_date.USDMXN as prev_usdmxn,
        date.USDMXN,
        ROUND(((date.USDMXN - prev_date.USDMXN)/prev_date.USDMXN)* 100, 2) AS prc_change
    FROM {{ ref("stg_cv_exchange_rates") }} AS prev_date
    JOIN (
        SELECT 
            DATE_VAL, 
            USDMXN 
        FROM {{ ref("stg_cv_exchange_rates") }}
    ) as date
    ON date.date_val -1 = prev_date.date_val
)

SELECT * FROM wk_exchange_prc_diff_day_by_day