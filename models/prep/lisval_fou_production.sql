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
    Generated : 2025-12-29 11:37:43
    Source    : ods.lisval_fou_production
Description : Liste fournisseurs production
    Rows ODS  : 5
    Cols ODS  : 80
    Cols PREP : 8 (+ _prep_loaded_at)
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
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lisval_fou_production') }}
    