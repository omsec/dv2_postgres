{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cse_customer_employee"
src_pk: "hk_customer_employee"
src_hashdiff:
  source_column: "rh_customer_employee"
  alias: "rowhash"
src_payload:
    - 'customer_bk'
    - 'employee_bk'
    - 'cod_role'
    - 'cse_rowid'
    - 'cse_created_at'
    - 'usr_created_by'
    - 'cse_modified_at'
    - 'usr_modified_by'
    - 'cse_deleted_at'
    - 'usr_deleted_by'
    - 'cse_valid_from'
    - 'cse_valid_to'
    - 'cse_primary'
src_eff: effective_from
src_ldts: "load_ts"
src_source: "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

-- using standard satellite macro instead of eff_sat,
-- since the eff_sat macro ignores any changes in payload

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}