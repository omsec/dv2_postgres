    -- *** GENERATED CODE ***
    -- build technical periods (load_ts / loadend_ts) depending on cardinality
    -- 1:0..1 & 1:*
    --  new combination means new period
    --  >> build technical period via driving key
select
	t.hk_customer_badge,
	t.hk_customer,
	t.hk_badge,
	t.load_ts,
	-- adjust as neeeded
	coalesce(lead(t.load_ts) over(partition by t.hk_customer order by t.load_ts) - interval '1 ms', to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts,
	t.record_source
from {{ ref('s_customer_badge') }} t
