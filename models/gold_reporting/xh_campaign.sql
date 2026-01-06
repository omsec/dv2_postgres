select
	t.*
	-- check if current_user() is member of a certain group; mask attributes as needed
from {{ ref('fh_campaign') }} t
