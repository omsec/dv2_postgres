select
	t.*
from {{ ref('fh_productcategory') }} t