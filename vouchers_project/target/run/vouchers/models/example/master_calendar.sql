
      

    insert into "vouchers"."public"."master_calendar" ("date", "day_of_week", "day_of_month", "week_of_month", "week_of_year", "month", "month_name", "year")
    (
       select "date", "day_of_week", "day_of_month", "week_of_month", "week_of_year", "month", "month_name", "year"
       from "master_calendar__dbt_tmp190619958577"
    );
  