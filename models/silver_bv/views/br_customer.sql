select
	t.*,
	-- add calculated attributes / business rules here
    extract(year from age(t.cst_birth_date)) as cst_age
from {{ ref('pf_customer') }} t
