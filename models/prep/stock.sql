{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'stock',
            'layer': 'prep'
        },
        'description': 'Stock par dépôt'
    },
    unique_key=['cod_pro', 'depot'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_stock_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_stock_pk_current ON {{ this }} USING btree (cod_pro, depot, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : stock
    ============================================================================
    Generated : 2025-12-31 12:04:44
    Source    : ods.stock
Description : Stock par dépôt
    Rows ODS  : 279,770
    Cols ODS  : 48
    Cols PREP : 34 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_pro" AS cod_pro,
    "depot" AS depot,
    "magasin" AS magasin,
    "emplac" AS emplac,
    "stock" AS stock,
    "pmp" AS pmp,
    "arr_sit" AS arr_sit,
    "pmp_sit" AS pmp_sit,
    "qte_sit" AS qte_sit,
    "cum_ent" AS cum_ent,
    "cum_sor" AS cum_sor,
    "dat_ent" AS dat_ent,
    "dat_sor" AS dat_sor,
    "val_ent" AS val_ent,
    "val_sor" AS val_sor,
    "poids" AS poids,
    "poi_sit" AS poi_sit,
    "px_rvt" AS px_rvt,
    "nb_mvt" AS nb_mvt,
    "px_std" AS px_std,
    "poids_net" AS poids_net,
    "poi_net_sit" AS poi_net_sit,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "px_sim" AS px_sim,
    "hr_ent" AS hr_ent,
    "hr_sor" AS hr_sor,
    "stk_mini" AS stk_mini,
    "stk_maxi" AS stk_maxi,
    "dat_cal" AS dat_cal,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'stock') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    