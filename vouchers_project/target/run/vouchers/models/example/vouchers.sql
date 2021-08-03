
      

    insert into "vouchers"."public"."vouchers" ("user_id", "voucher_code", "product", "country", "vendor", "status", "date")
    (
       select "user_id", "voucher_code", "product", "country", "vendor", "status", "date"
       from "vouchers__dbt_tmp190619706880"
    );
  