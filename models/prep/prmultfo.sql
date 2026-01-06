{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'prmultfo',
            'layer': 'prep'
        },
        'description': 'Produits multi-fournisseurs'
    },
    unique_key=['cod_pro', 'cod_fou'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_prmultfo_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_prmultfo_pk_current ON {{ this }} USING btree (cod_pro, cod_fou, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : prmultfo
    ============================================================================
    Generated : 2025-12-31 12:04:38
    Source    : ods.prmultfo
Description : Produits multi-fournisseurs
    Rows ODS  : 11,536
    Cols ODS  : 170
    Cols PREP : 52 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "cod_pro" AS cod_pro,
    "cod_fou" AS cod_fou,
    "des_ach" AS des_ach,
    "delai" AS delai,
    "refext" AS refext,
    "devise" AS devise,
    "px_refa" AS px_refa,
    "fpx_refa" AS fpx_refa,
    "dat_fpxa" AS dat_fpxa,
    "uni_ach" AS uni_ach,
    "lib_unia" AS lib_unia,
    "rem_ach" AS rem_ach,
    "gencod-a" AS gencod_a,
    "typ_elem" AS typ_elem,
    "nom_pro" AS nom_pro,
    "refint" AS refint,
    "principa" AS principa,
    "statut" AS statut,
    "conv_md" AS conv_md,
    "longueur" AS longueur,
    "largeur" AS largeur,
    "hauteur" AS hauteur,
    "calc_uv" AS calc_uv,
    "zal_1" AS zal_1,
    "zal_5" AS zal_5,
    "znu_1" AS znu_1,
    "znu_3" AS znu_3,
    "znu_4" AS znu_4,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "zta_4" AS zta_4,
    "fam_tar" AS fam_tar,
    "sf_tar" AS sf_tar,
    "cod_conv" AS cod_conv,
    "s3_tar" AS s3_tar,
    "prio_fou" AS prio_fou,
    "dat_prin" AS dat_prin,
    "mini_ach" AS mini_ach,
    "mult_ach" AS mult_ach,
    "nb_ua" AS nb_ua,
    "pp_ua" AS pp_ua,
    "phone" AS phone,
    "px_max" AS px_max,
    "art_cs_u" AS art_cs_u,
    "art_cs_c" AS art_cs_c,
    "dat_import" AS dat_import,
    "qte_eco" AS qte_eco,
    "pays_ori" AS pays_ori,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'prmultfo') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    