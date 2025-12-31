{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'tabgco',
            'layer': 'prep'
        },
        'description': 'Table générique GCO'
    },
    unique_key='_etl_hashdiff',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_tabgco_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : tabgco
    ============================================================================
    Generated : 2025-12-31 12:04:45
    Source    : ods.tabgco
Description : Table générique GCO
    Rows ODS  : 6,118
    Cols ODS  : 289
    Cols PREP : 81 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "ndos" AS ndos,
    "n_tab" AS n_tab,
    "a_tab" AS a_tab,
    "type_tab" AS type_tab,
    "inti_tab" AS inti_tab,
    "fa_prcm" AS fa_prcm,
    "fp_cptv" AS fp_cptv,
    "fp_cpta" AS fp_cpta,
    "zal_1" AS zal_1,
    "zal_2" AS zal_2,
    "znu_1" AS znu_1,
    "znu_2" AS znu_2,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "civilite" AS civilite,
    "adresse_1" AS adresse_1,
    "adresse_2" AS adresse_2,
    "adresse_3" AS adresse_3,
    "ville" AS ville,
    "pays" AS pays,
    "cli_cai" AS cli_cai,
    "dern_z" AS dern_z,
    "typ_int_1" AS typ_int_1,
    "typ_int_2" AS typ_int_2,
    "typ_int_3" AS typ_int_3,
    "typ_int_4" AS typ_int_4,
    "typ_int_5" AS typ_int_5,
    "typ_int_6" AS typ_int_6,
    "typ_int_7" AS typ_int_7,
    "typ_int_8" AS typ_int_8,
    "typ_int_9" AS typ_int_9,
    "typ_int_10" AS typ_int_10,
    "productif" AS productif,
    "nb_int" AS nb_int,
    "cod_equ" AS cod_equ,
    "no_tel_1" AS no_tel_1,
    "no_tel_2" AS no_tel_2,
    "no_tel_3" AS no_tel_3,
    "no_fax_1" AS no_fax_1,
    "no_fax_2" AS no_fax_2,
    "no_fax_3" AS no_fax_3,
    "type_tf_1" AS type_tf_1,
    "mel" AS mel,
    "no_port" AS no_port,
    "min_t" AS min_t,
    "jour_t_1" AS jour_t_1,
    "jour_t_2" AS jour_t_2,
    "jour_t_3" AS jour_t_3,
    "jour_t_4" AS jour_t_4,
    "jour_t_5" AS jour_t_5,
    "jour_t_6" AS jour_t_6,
    "heure_t_1" AS heure_t_1,
    "heure_t_2" AS heure_t_2,
    "heure_t_3" AS heure_t_3,
    "heure_t_4" AS heure_t_4,
    "adresse4" AS adresse4,
    "fct_com" AS fct_com,
    "fct_tlv" AS fct_tlv,
    "fct_aa" AS fct_aa,
    "fct_ce" AS fct_ce,
    "cod_tlv" AS cod_tlv,
    "externe" AS externe,
    "usr_pro" AS usr_pro,
    "cod_cli" AS cod_cli,
    "cod_fou" AS cod_fou,
    "fp_cptr" AS fp_cptr,
    "fp_cptt" AS fp_cptt,
    "texte" AS texte,
    "k_post2" AS k_post2,
    "WEB" AS web,
    "datent" AS datent,
    "nobur" AS nobur,
    "secta_imp_1" AS secta_imp_1,
    "secta_imp_2" AS secta_imp_2,
    "non_sscc" AS non_sscc,
    "non_sor_sscc" AS non_sor_sscc,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_hashdiff" AS _etl_hashdiff,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'tabgco') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    