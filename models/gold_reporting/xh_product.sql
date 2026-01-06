select
	t.*
from {{ ref('fh_product') }} t
