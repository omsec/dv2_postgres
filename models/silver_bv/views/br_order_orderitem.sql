-- View containing Business Rules
select
    t.hk_order_orderitem,
    t.load_ts,
    t.loadend_ts,
    -- denorm/extension of the DV2-Standard
    t.hk_order,
    t.hk_product,
    t.hk_warehouse,
    t.rowhash,
    -- payload
    t.oit_rowid,
    t.oit_created_at,
    t.usr_created_by,
    t.usr_created_by_login_name,
    t.oit_modified_at,
    t.usr_modified_by,
    t.usr_modified_by_login_name,
    t.oit_deleted_at,
    t.ord_order,
    t.prd_product,
    t.wrh_warehouse,
    t.usr_deleted_by,
    t.usr_deleted_by_login_name,
    -- payload & business rules
    t.oit_quantity,
    coalesce(t.oit_unit_price, 0) as oit_unit_price,
    t.oit_attr_int,
    t.oit_attr_str,
    t.oit_ord_start,
    t.oit_ord_end,
    t.oit_ord_effective_from
    --t.record_source
from {{ ref('fd_order_orderitem') }} t