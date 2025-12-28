select
    -- *:* link
    -- technical periods (load_ts/loadend_ts) are built over the hashed combinations.
    -- multiple combinations may exist at any given point in time.
    -- query logics:
    --      1. read a referenced key (hub, eg. cst) from the link
    --      2. filter active combinations via satellite (load_ts/loadend_ts)
    --      3. filter valid combinations via satellite's payload (eg. valid_from/valid_to) as provided by the source system
    t.*,
    coalesce(lead(t.load_ts) over(partition by t.hk_customer_employee order by t.load_ts) - interval '1 ms', to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts
from {{ ref('s_customer_employee') }} t