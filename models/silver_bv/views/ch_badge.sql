with ts as (
	select
		-- enable this for use in dimension tables
		/*decode(md5(concat_ws('||'::text,
		coalesce(nullif(trim(both from t.hk_badge::character varying), ''::text), '^^'::text),
		coalesce(nullif(trim(both from t.load_ts::character varying), ''::text), '^^'::text)
		)), 'hex'::text) as hk_badge_snapshot,*/
		t.hk_badge,
		t.bdg_badge_bk,
		t.rowhash,
		t.bdg_rowid,
		t.bdg_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.bdg_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.bdg_deleted_at) over(partition by t.hk_badge order by t.load_ts) as bdg_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_badge order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_badge order by t.load_ts) as usr_deleted_by_login_name,
		t.bdg_name,
		t.bdg_type1,
		t.bdg_type2,
		t.bdg_abilities,
		t.bdg_generation,
		t.load_ts,
		coalesce(
			t.bdg_deleted_at,
			lead(t.bdg_deleted_at) over(partition by t.hk_badge order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_badge order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts
	from {{ ref('fd_badge') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
