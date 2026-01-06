{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'crnar',
            'layer': 'prep'
        },
        'description': 'Articles concurrents'
    },
    unique_key=['cod_crn', 'cod_pc'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'crnar') }} s WHERE s.cod_crn = t.cod_crn AND s.cod_pc = t.cod_pc AND s._etl_is_current = TRUE){% endif %}",
        "CREATE INDEX IF NOT EXISTS idx_crnar_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : crnar
    ============================================================================
    Generated : 2025-12-30 16:59:58
    Source    : ods.crnar
Description : Articles concurrents
    Rows ODS  : 384,320
    Cols ODS  : 59
    Cols PREP : 25 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_crn" AS cod_crn,
    "cod_pc" AS cod_pc,
    "nom_pc" AS nom_pc,
    "ref_pc" AS ref_pc,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_4" AS zal_4,
    "zta_1" AS zta_1,
    "zda_3" AS zda_3,
    "px_vte" AS px_vte,
    "marque" AS marque,
    "zlo_1" AS zlo_1,
    "texte" AS texte,
    "pp_uv" AS pp_uv,
    "cod_pro" AS cod_pro,
    "poid_brut_1" AS poid_brut_1,
    "poid_brut_2" AS poid_brut_2,
    "poid_brut_3" AS poid_brut_3,
    "gencod-v" AS gencod_v,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    "_etl_is_current" AS _etl_is_current,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'crnar') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    