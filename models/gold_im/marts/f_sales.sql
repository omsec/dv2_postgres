-- By not specifing a unique key, dbt won't update old date; hence facts are "insert only" models

-- ToDo:
-- appearently the above statement is not true. this adds a record for any run :-/

-- https://docs.getdbt.com/docs/build/incremental-strategy

-- incremental_strategy='append',
-- unique_key=['d_date', 'd_product', 'd_customer', 'd_salesman']
-- >>> could not write to file "base/pgsql_tmp/pgsql_tmp6168.69": No space left on device
-- increased mem config, see below

-- FÜR DEN MOMENT 'TABLE'
--> order: vs (änderungen am status erkennen)
--> orderitems (bzw. referenzen) letzter stand; fachlich zu definieren (änderung ja nur im status 'new' erlaubt)

-- In degenrated facts, also include keys of the integrated dims (order)

{{
    config(
        materialized='incremental',
		unique_key=['d_date', 'hk_order', 'd_product', 'd_customer', 'd_salesman']
    )
}}

select
	--to_char(xOrd.ord_sale_ts, 'yyyymmdd')::int as d_date,
	to_char(coalesce(xOrd.ord_modified_at, xOrd.ord_created_at), 'yyyymmdd')::int as d_date, -- this is more acurate than load_ts
	xPrd.hk_product_snapshot as d_product,
	xCst.hk_customer_snapshot as d_customer,
	xEmpSm.hk_employee_snapshot as d_salesman,
	xPrd.product_bk,
	xCst.cst_customer_no,
	xEmpSm.emp_employee_no as emp_salesman,
	xOrd.hk_order,
	xOrd.ord_order_no,
	xOrd.ord_sale_ts,
	xOrd.txt_status_en,
	sum(sOrdOit.oit_quantity) as sum_quantity,
	max(sOrdOit.oit_unit_price) as oit_unit_price,
	sum(sOrdOit.oit_quantity * sOrdOit.oit_unit_price) as sum_unit_price
from {{ ref('l_order_customer_salesman') }} lOrdCstSm
join {{ ref('vls_order_customer_salesman') }} sOrdCstSm -- not unsafe; simple effSat
	on sOrdCstSm.hk_order_customer_salesman = lOrdCstSm.hk_order_customer_salesman 
join {{ ref('al_order') }} xOrd
	on xOrd.hk_order = lOrdCstSm.hk_order
join {{ ref('al_customer') }} xCst
	on xCst.hk_customer = lOrdCstSm.hk_customer
join {{ ref('al_employee') }} xEmpSm
	on xEmpSm.hk_employee = lOrdCstSm.hk_salesman
join {{ ref('l_order_orderitem') }} lOrdOit
	on lOrdOit.hk_order = xOrd.hk_order
join {{ ref('al_order_orderitem') }} sOrdOit
	on sOrdOit.hk_order_orderitem = lOrdOit.hk_order_orderitem
join {{ ref('al_product') }} xPrd
	on xPrd.hk_product = lOrdOit.hk_product
--where
    -- status model:
    -- new / {cancelled} / in progress / done
    -- * can't return to new after any modification
    -- * modifications only in status NEW
    -- sOrd.cod_status = '10' -- new
    --and hOrd.ord_order_no = 'ord2018.05/3457.6-75_500_354'
group by
	to_char(coalesce(xOrd.ord_modified_at, xOrd.ord_created_at), 'yyyymmdd')::int,
	xPrd.hk_product_snapshot,
	xCst.hk_customer_snapshot,
	xEmpSm.hk_employee_snapshot,
	xPrd.product_bk,
	xCst.cst_customer_no,
	xEmpSm.emp_employee_no,
	xOrd.hk_order,
	xOrd.ord_order_no,
	xOrd.ord_sale_ts,
	xOrd.txt_status_en

{% if is_incremental() %}
	-- only insert **new** records
	where not exists (
		select 1
		from {{ this }} t
		where
			t.d_date = src.d_date
			and t.d_product = src.d_product
			and t.d_customer = src.d_customer
			and t.d_salesman = src.d_salesman
	)
{% endif %}


/*	
this let the postgresql17-server (default config) crash
either remove it or increase memory consumption limits.
the settings also speed-up performance.

sudo vi /var/lib/pgsql/17/data/postgresql.conf

work_mem = 64MB
maintenance_work_mem = 128MB

sudo systemctl restart postgresql-17

removed the order by anyway :P

order by
	d_date,
	cast(bPrd.product_bk as int)
*/	    