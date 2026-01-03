    -- *:*
    --  new combination means additional period
    --  >> build technical period over HK combination itself (using its HK)
select
	t.hk_order_customer_salesman,
	t.hk_order,
	t.hk_customer,
	t.hk_salesman,
	t.load_ts,
	-- adjust as neeeded
	coalesce(lead(t.load_ts) over(partition by t.hk_order_customer_salesman order by t.load_ts) - interval '1 ms', to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts,
	t.record_source
from {{ ref('s_order_customer_salesman') }} t
