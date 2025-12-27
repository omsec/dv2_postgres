select
	pit.hk_customer_snapshot,
	pit.hk_customer,
	pit.cst_customer_no,
	pit.load_ts,
	pit.loadend_ts,
	-- v_customer
	sat1.cod_gender,
	coalesce(cdGenderEN.cod_text, sat1.cod_gender) as txt_gender_en,
	sat1.cst_first_name,
	sat1.cst_last_name,
	sat1.cst_birth_date,
	sat1.cod_language,
	coalesce(cdLanguageEN.cod_text, sat1.cod_language) as txt_language_en,
	sat1.cst_culture,
	sat1.cst_credit_limit,
	sat1.load_ts as sat1_load_ts,
	sat1.loadend_ts as sat1_loadend_ts,
	-- v_customer_extended
	sat3.cst_remark,
	sat3.cst_attr1,
	sat3.cst_attr2,
	sat3.cst_attr3,
	sat3.cst_attr_int,
	sat3.cst_attr_str,
	sat3.load_ts as sat3_load_ts,
	sat3.loadend_ts as sat3_loadend_ts,
	-- v_customer_meta
	sat4.cst_rowid,
	sat4.cst_created_at,
	sat4.usr_created_by,
	coalesce(usrC.usr_login_name, sat4.usr_created_by) as usr_created_by_login_name,
	sat4.cst_modified_at,
	sat4.usr_modified_by,
	coalesce(usrM.usr_login_name, sat4.usr_modified_by) as usr_modified_by_login_name,
	sat4.cst_deleted_at,
	sat4.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat4.usr_deleted_by) as usr_deleted_by_login_name,
	sat4.load_ts as sat4_load_ts,
	sat4.loadend_ts as sat4_loadend_ts
from {{ ref('p_customer') }} as pit
-- using outer joins, since we deal with different history-lines
left outer join {{ ref('v_customer') }} as sat1
	on  sat1.hk_customer = pit.hk_customer
	and sat1.load_ts = pit.sat1_load_ts
left outer join {{ ref('v_customer_extended') }} as sat3
	on  sat3.hk_customer = pit.hk_customer
	and sat3.load_ts = pit.sat3_load_ts
left outer join {{ ref('v_customer_meta') }} as sat4
	on  sat4.hk_customer = pit.hk_customer
	and sat4.load_ts = pit.sat4_load_ts
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat4.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrC.usr_rowid = sat4.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrC.usr_rowid = sat4.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('v_codedefinition')}} cdGenderEN
	on  cdGenderEN.cog_group = 1
	and cdGenderEN.cod_value = sat1.cod_gender
	and cdGenderEN.cod_language = 10
	and cdGenderEN.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
left outer join {{ ref('v_codedefinition')}} cdLanguageEN
	on  cdLanguageEN.cog_group = 2
	and cdLanguageEN.cod_value = sat1.cod_language
	and cdLanguageEN.cod_language = 10
	and cdLanguageEN.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
