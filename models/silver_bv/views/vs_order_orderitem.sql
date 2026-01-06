    -- *:*
    --  new combination means additional period
    --  >> build technical period over HK combination itself (using its HK)
select
    t.hk_order_orderitem,
    t.load_ts,
    -- adjust as neeeded
    coalesce(lead(t.load_ts) over(partition by t.hk_order_orderitem order by t.load_ts) - interval '1 ms', to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts,
    t.rowhash,
    -- denorm/extension of the DV2-Standard
    t.hk_order,
    t.hk_product,
    t.hk_warehouse,
    -- payload
    t.oit_rowid,
    t.oit_created_at,
    t.usr_created_by,
    t.oit_modified_at,
    t.usr_modified_by,
    t.oit_deleted_at,
    t.usr_deleted_by,
    t.ord_order,
    t.prd_product,
    t.wrh_warehouse,
    t.oit_quantity,
    t.oit_unit_price,
    t.oit_attr_int,
    t.oit_attr_str,
    t.oit_ord_start,
    t.oit_ord_end,
    t.oit_ord_effective_from,
    t.record_source
from {{ ref('s_order_orderitem') }} t