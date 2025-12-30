{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_prd_product"
src_pk: "HK_PRODUCT_PRODUCTCATEGORY"
src_hashdiff:
  source_column: "RH_PRODUCT_PRODUCTCATEGORY"
  alias: "ROWHASH"
src_payload:
    - 'HK_PRODUCT'
    - 'HK_PRODUCTCATEGORY'
    - 'prd_category_valid_from'
    - 'prd_category_valid_to'
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