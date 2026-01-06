{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'deppro',
            'layer': 'prep'
        },
        'description': 'Infos produits par dépôt'
    },
    unique_key=['cod_fou', 'cod_pro', 'depot'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_deppro_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_deppro_pk_current ON {{ this }} USING btree (cod_fou, cod_pro, depot, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : deppro
    ============================================================================
    Generated : 2025-12-31 12:04:34
    Source    : ods.deppro
Description : Infos produits par dépôt
    Rows ODS  : 2,815,334
    Cols ODS  : 104
    Cols PREP : 20 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_pro" AS cod_pro,
    "depot" AS depot,
    "cod_fou" AS cod_fou,
    "dat_prin" AS dat_prin,
    "principa" AS principa,
    "px_mini" AS px_mini,
    "dat_ent" AS dat_ent,
    "px_rvt" AS px_rvt,
    "px_std" AS px_std,
    "px_refv" AS px_refv,
    "dat_fpxv" AS dat_fpxv,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "px_sim" AS px_sim,
    "px_ttc" AS px_ttc,
    "dat_remp" AS dat_remp,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'deppro') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    