{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'soumission',
            'layer': 'prep'
        },
        'description': 'Contrats fournisseurs'
    },
    unique_key='uniq_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_soumission_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_soumission_pk_current ON {{ this }} USING btree (uniq_id, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : soumission
    ============================================================================
    Generated : 2025-12-31 12:04:50
    Source    : ods.soumission
Description : Contrats fournisseurs
    Rows ODS  : 185,441
    Cols ODS  : 152
    Cols PREP : 31 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "no_contrat" AS no_contrat,
    "lib_contrat" AS lib_contrat,
    "cod_tiers" AS cod_tiers,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "cod_pro" AS cod_pro,
    "rem_app_1" AS rem_app_1,
    "rem_app_2" AS rem_app_2,
    "px_net_1" AS px_net_1,
    "px_net_2" AS px_net_2,
    "px_net_3" AS px_net_3,
    "px_net_4" AS px_net_4,
    "cod_dec1" AS cod_dec1,
    "cod_dec2" AS cod_dec2,
    "cod_dec3" AS cod_dec3,
    "cod_dec4" AS cod_dec4,
    "cod_dec5" AS cod_dec5,
    "qte" AS qte,
    "typ_con" AS typ_con,
    "qte_his" AS qte_his,
    "cat_tar" AS cat_tar,
    "depot" AS depot,
    "no_lot" AS no_lot,
    "zta_1" AS zta_1,
    "uniq_id" AS uniq_id,
    "dat_liv" AS dat_liv,
    "no_cde" AS no_cde,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'soumission') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    