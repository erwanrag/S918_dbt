{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'prnomenc',
            'layer': 'prep'
        },
        'description': 'Lignes nomenclature produits'
    },
    unique_key=['cod_nmc', 'cod_pro', 'depot', 'ordre', 'type_nmc'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_prnomenc_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_prnomenc_pk_current ON {{ this }} USING btree (cod_nmc, cod_pro, depot, ordre, type_nmc, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : prnomenc
    ============================================================================
    Generated : 2025-12-31 12:04:39
    Source    : ods.prnomenc
Description : Lignes nomenclature produits
    Rows ODS  : 18,965
    Cols ODS  : 135
    Cols PREP : 29 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_pro" AS cod_pro,
    "type_nmc" AS type_nmc,
    "ordre" AS ordre,
    "quantite" AS quantite,
    "cod_nmc" AS cod_nmc,
    "niveau" AS niveau,
    "editer" AS editer,
    "sor_comp" AS sor_comp,
    "cod_dec1" AS cod_dec1,
    "cod_dec2" AS cod_dec2,
    "cod_dec3" AS cod_dec3,
    "cod_dec4" AS cod_dec4,
    "cod_dec5" AS cod_dec5,
    "depot" AS depot,
    "condition" AS condition,
    "dat_app" AS dat_app,
    "dat_fin" AS dat_fin,
    "qte_avc" AS qte_avc,
    "qte_pf" AS qte_pf,
    "no_page" AS no_page,
    "date_deb" AS date_deb,
    "date_fin" AS date_fin,
    "no_info" AS no_info,
    "nb_cs3" AS nb_cs3,
    "cod_fou" AS cod_fou,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'prnomenc') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    