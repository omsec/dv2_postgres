select
    t.*
from {{ ref('vs_order_customer_salesman') }} t