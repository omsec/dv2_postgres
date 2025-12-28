{%- set yaml_metadata -%}
source_model: 'cse_customer_employee'
derived_columns:
  record_source: '!cse_customer_employee'
  load_ts: coalesce(cse_deleted_at, cse_modified_at, cse_created_at) + interval '1 day'
  start_dt: cse_valid_from
  end_dt: cse_valid_to
  effective_from: coalesce(cse_modified_at, cse_created_at)
hashed_columns:
  hk_customer_employee:
    - 'customer_bk'
    - 'employee_bk'
    - 'cod_role'
  hk_customer: 'customer_bk'
  hk_employee: 'employee_bk'
  rh_customer_employee:
    is_hashdiff: true
    columns:
      - 'cse_created_at'
      - 'usr_created_by'
      - 'cse_modified_at'
      - 'usr_modified_by'
      - 'cse_deleted_at'
      - 'usr_deleted_by'
      - 'cse_valid_from'
      - 'cse_valid_to'
      - 'cse_primary'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}