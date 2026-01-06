{{ config(
    materialized='table',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'lisval_produits_vehicules',
            'layer': 'prep'
        },
        'description': 'Produits véhicules'
    },
) }}

    /*
    ============================================================================
    PREP MODEL : lisval_produits_vehicules
    ============================================================================
    Generated : 2025-12-30 17:00:16
    Source    : ods.lisval_produits_vehicules
Description : Produits véhicules
    Rows ODS  : 180,905
    Cols ODS  : 83
    Cols PREP : 13 (+ _prep_loaded_at)
    Strategy  : TABLE
    ============================================================================
    */

    SELECT
        "typ_fich" AS typ_fich,
    "liste" AS liste,
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "zt1" AS zt1,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    "_etl_is_current" AS _etl_is_current,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'lisval_produits_vehicules') }}
    