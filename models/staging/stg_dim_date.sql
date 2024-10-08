select date,
       date_key,
       day,
       day_of_week,
       day_of_week_name_eeee,
       day_of_week_name_ee,
       day_of_month,
       day_of_year,
       day_number_string,
       week,
       week_name_long,
       week_name_short,
       week_number_string,
       week_start_date,
       week_end_date,
       month,
       month_name_mmmm,
       month_name_mmm,
       month_number_mm,
       month_year_dash_yy,
       month_year_index,
       quarter,
       quarter_name_qqqq,
       quarter_name_qqq,
       quarter_number_qq,
       quarter_year_dash_yy,
       quarter_year_index,
       year_quarter_yyyy_qq,
       year,
       year_index,
       year_month_yyyy_mm,
       month_start_date,
       month_end_date,
       loaded_at
from {{ source ('raw', 'raw_dim_date')}}