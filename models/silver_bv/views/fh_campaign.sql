with ts as (
	select
		-- enable this for use in dimension tables
		/*decode(md5(concat_ws('||'::text,
		coalesce(nullif(trim(both from t.hk_campaign::character varying), ''::text), '^^'::text),
		coalesce(nullif(trim(both from t.load_ts::character varying), ''::text), '^^'::text)
		)), 'hex'::text) as hk_campaign_snapshot,*/
		t.hk_campaign,
		t.cmp_campaign_bk,
		t.cmp_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.cmp_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.cmp_deleted_at) over(partition by t.hk_campaign order by t.load_ts) as cmp_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_campaign order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_campaign order by t.load_ts) as usr_deleted_by_login_name,
		t.cmp_channel,
		t.cmp_description,
		t.load_ts,
		coalesce(
			t.cmp_deleted_at,
			lead(t.cmp_deleted_at) over(partition by t.hk_campaign order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_campaign order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts
	from {{ ref('fd_campaign') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
