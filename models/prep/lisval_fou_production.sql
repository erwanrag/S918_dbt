{{ config(
    materialized='table',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'lisval_fou_production',
            'layer': 'prep'
        },
        'description': 'Liste fournisseurs production'
    },
) }}

    /*
    ============================================================================
    PREP MODEL : lisval_fou_production
    ============================================================================
    Generated : 2025-12-30 17:00:13
    Source    : ods.lisval_fou_production
Description : Liste fournisseurs production
    Rows ODS  : 5
    Cols ODS  : 83
    Cols PREP : 9 (+ _prep_loaded_at)
    Strategy  : TABLE
    ============================================================================
    */

    SELECT
        "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    "_etl_is_current" AS _etl_is_current,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lisval_fou_production') }}
    