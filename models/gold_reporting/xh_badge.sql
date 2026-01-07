select
	t.*
	-- check if current_user() is member of a certain group; mask attributes as needed
from {{ ref('ch_badge') }} t
