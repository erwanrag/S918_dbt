{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'crn',
            'layer': 'prep'
        },
        'description': 'Sociétés concurrentes'
    },
    unique_key='cod_crn',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_crn_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_crn_pk_current ON {{ this }} USING btree (cod_crn, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : crn
    ============================================================================
    Generated : 2025-12-31 12:04:27
    Source    : ods.crn
Description : Sociétés concurrentes
    Rows ODS  : 837
    Cols ODS  : 59
    Cols PREP : 16 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_crn" AS cod_crn,
    "nom_crn" AS nom_crn,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "num_tel" AS num_tel,
    "num_fax" AS num_fax,
    "no_info" AS no_info,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'crn') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    