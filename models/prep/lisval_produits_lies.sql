{{ config(
    materialized='table',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'lisval_produits_lies',
            'layer': 'prep'
        },
        'description': 'Produits liés'
    },
) }}

    /*
    ============================================================================
    PREP MODEL : lisval_produits_lies
    ============================================================================
    Generated : 2025-12-30 15:27:26
    Source    : ods.lisval_produits_lies
Description : Produits liés
    Rows ODS  : 4,488
    Cols ODS  : 83
    Cols PREP : 10 (+ _prep_loaded_at)
    Strategy  : TABLE
    ============================================================================
    */

    SELECT
        "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    "_etl_is_current" AS _etl_is_current,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lisval_produits_lies') }}
    