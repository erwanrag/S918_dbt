{{ config(
    materialized='table',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'lisval_slimstock',
            'layer': 'prep'
        },
        'description': 'Liste Slimstock'
    },
) }}

    /*
    ============================================================================
    PREP MODEL : lisval_slimstock
    ============================================================================
    Generated : 2025-12-30 17:00:18
    Source    : ods.lisval_slimstock
Description : Liste Slimstock
    Rows ODS  : 197,849
    Cols ODS  : 83
    Cols PREP : 11 (+ _prep_loaded_at)
    Strategy  : TABLE
    ============================================================================
    */

    SELECT
        "cod_tiers" AS cod_tiers,
    "za0" AS za0,
    "zl0" AS zl0,
    "zt0" AS zt0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    "_etl_is_current" AS _etl_is_current,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lisval_slimstock') }}
    