WITH int_exchange_diff_week_by_week AS (
    SELECT 
        week_comparison_text,
        prev_first_week_day,
        curr_first_week_day,

        ROUND(curr_last_val, 2),
        ROUND(prev_last_val, 2),
        ROUND(diff_last_val, 2),
        ROUND(percentage_diff_last_val, 2),
        ROUND(curr_max_val, 2),
        ROUND(prev_max_val, 2),
        ROUND(diff_max_val, 2),
        ROUND(percentage_diff_max_val, 2),
        ROUND(curr_AVG_VAL, 2),
        ROUND(prev_AVG_VAL, 2),
        ROUND(diff_avg_val, 2),
        ROUND(percentage_diff_AVG_VAL, 2),
        SLOPE
    FROM {{ ref("int_exchange_diff_week_by_week") }}
)

SELECT * FROM int_exchange_diff_week_by_week