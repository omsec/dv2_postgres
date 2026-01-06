-- SCD2 by design (incremental)

{{
    config(
        materialized='incremental',
        unique_key='d_customer'
    )
}}

-- NOTE:
-- last version of COD is applied by the BV-views (fd) for all states

select
	t.hk_customer_snapshot as d_customer,
	t.hk_customer,
	t.cst_customer_no,
	t.load_ts,
	t.loadend_ts,
	t.cst_first_name,
	t.cst_last_name,
	t.txt_gender_en,
	t.txt_language_en,
	--sCst.cst_birth_date, -- redacted for data protection
	t.cst_age,
	t.cst_credit_limit,
	t.cst_remark,
	t.cst_attr1,
	t.cst_attr2,
	t.cst_attr3
from {{ ref('fh_customer') }} t
order by
	t.load_ts,
	t.loadend_ts