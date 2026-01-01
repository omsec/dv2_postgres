{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_oit_orderitem"
src_pk: "hk_order_orderitem"
src_hashdiff:
  source_column: "rh_ord_oit"
  alias: "rowhash"
src_payload:
    - 'hk_order'
    - 'hk_product'
    - 'hk_warehouse'
    - 'ord_order'
    - 'prd_product'
    - 'wrh_warehouse'
    - 'oit_rowid'
    - 'oit_created_at'
    - 'usr_created_by'
    - 'oit_modified_at'
    - 'usr_modified_by'
    - 'oit_deleted_at'
    - 'usr_deleted_by'
    - 'oit_quantity'
    - 'oit_unit_price'
    - 'oit_attr_int'
    - 'oit_attr_str'
    - 'oit_ord_start'
    - 'oit_ord_end'
src_eff: "oit_ord_effective_from"
src_ldts: "load_ts"
src_source: "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}