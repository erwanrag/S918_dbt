{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'lldespro',
            'layer': 'prep'
        },
        'description': 'Descriptions multilingues produits'
    },
    unique_key=['cod_pro', 'langue'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_lldespro_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_lldespro_pk_current ON {{ this }} USING btree (cod_pro, langue, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : lldespro
    ============================================================================
    Generated : 2025-12-31 12:04:37
    Source    : ods.lldespro
Description : Descriptions multilingues produits
    Rows ODS  : 551,627
    Cols ODS  : 12
    Cols PREP : 8 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_pro" AS cod_pro,
    "langue" AS langue,
    "des_lan" AS des_lan,
    "des_lan2" AS des_lan2,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lldespro') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    