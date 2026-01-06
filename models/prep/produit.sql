{{ config(
    materialized='incremental',
    meta = {
        'dagster': {
            'group': 'prep',
            'domain': 'produit',
            'layer': 'prep'
        },
        'description': 'Table des produits'
    },
    unique_key='cod_pro',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_produit_current ON {{ this }} USING btree (_etl_is_current) WHERE (_etl_is_current = true)",
        "CREATE INDEX IF NOT EXISTS idx_produit_pk_current ON {{ this }} USING btree (cod_pro, _etl_is_current)",
        "ANALYZE {{ this }}"
    ]
) }}

    /*
    ============================================================================
    PREP MODEL : produit
    ============================================================================
    Generated : 2025-12-31 12:04:42
    Source    : ods.produit
Description : Table des produits
    Rows ODS  : 2,833
    Cols ODS  : 616
    Cols PREP : 110 (+ _prep_loaded_at)
    Strategy  : INCREMENTAL
    ============================================================================
    */

    SELECT
        "sous_type" AS sous_type,
    "typ_elem" AS typ_elem,
    "cod_pro" AS cod_pro,
    "nom_pro" AS nom_pro,
    "nom_pr2" AS nom_pr2,
    "refint" AS refint,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_1" AS zal_1,
    "znu_3" AS znu_3,
    "znu_5" AS znu_5,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "zta_4" AS zta_4,
    "zda_2" AS zda_2,
    "statut" AS statut,
    "groupe" AS groupe,
    "famille" AS famille,
    "s_famille" AS s_famille,
    "fam_cpt" AS fam_cpt,
    "calcul" AS calcul,
    "ordre" AS ordre,
    "px_refv" AS px_refv,
    "coef_t2" AS coef_t2,
    "coef_t3" AS coef_t3,
    "coef_t4" AS coef_t4,
    "dat_fpxv" AS dat_fpxv,
    "lib_univ" AS lib_univ,
    "mult-fou" AS mult_fou,
    "refext" AS refext,
    "px_refa" AS px_refa,
    "fam_tar" AS fam_tar,
    "marque" AS marque,
    "art_remp" AS art_remp,
    "dat_remp" AS dat_remp,
    "classe" AS classe,
    "coef_dep" AS coef_dep,
    "rem_vte" AS rem_vte,
    "pmp" AS pmp,
    "ctrl_qua" AS ctrl_qua,
    "cod_for" AS cod_for,
    "qte_for" AS qte_for,
    "cod_nom" AS cod_nom,
    "poid_net" AS poid_net,
    "gencod-v" AS gencod_v,
    "consigne" AS consigne,
    "art_cs_u" AS art_cs_u,
    "poid_brut_1" AS poid_brut_1,
    "largeur_1" AS largeur_1,
    "longueur_1" AS longueur_1,
    "hauteur_1" AS hauteur_1,
    "d_pxach" AS d_pxach,
    "dat_ent" AS dat_ent,
    "ges_stk" AS ges_stk,
    "ges_dem" AS ges_dem,
    "px_rvt" AS px_rvt,
    "px_refav" AS px_refav,
    "dpx_rvt" AS dpx_rvt,
    "sf_tar" AS sf_tar,
    "cod_par" AS cod_par,
    "px_poi" AS px_poi,
    "zlo_1" AS zlo_1,
    "zlo_2" AS zlo_2,
    "cod_cli" AS cod_cli,
    "int_condt" AS int_condt,
    "gar_fab" AS gar_fab,
    "qte_eco" AS qte_eco,
    "s3_famille" AS s3_famille,
    "qte_serm" AS qte_serm,
    "ach_ctr" AS ach_ctr,
    "nb_ctr" AS nb_ctr,
    "px_sim" AS px_sim,
    "qte_max" AS qte_max,
    "cod_conv" AS cod_conv,
    "cod_cal_1" AS cod_cal_1,
    "cod_cal_2" AS cod_cal_2,
    "cod_cal_3" AS cod_cal_3,
    "cod_cal_4" AS cod_cal_4,
    "cod_cal_5" AS cod_cal_5,
    "s3_tar" AS s3_tar,
    "volume_1" AS volume_1,
    "px_blq" AS px_blq,
    "px_mini" AS px_mini,
    "ges_emp" AS ges_emp,
    "mini_vte" AS mini_vte,
    "mult_vte" AS mult_vte,
    "cod_nue" AS cod_nue,
    "nb_uv" AS nb_uv,
    "cod_prx" AS cod_prx,
    "px_std" AS px_std,
    "w_produit" AS w_produit,
    "phone" AS phone,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "cod_cat" AS cod_cat,
    "px_ttc" AS px_ttc,
    "dat_import" AS dat_import,
    "non_sscc" AS non_sscc,
    "px_etud" AS px_etud,
    "non_uli" AS non_uli,
    "pds_net_1" AS pds_net_1,
    "cod_prev" AS cod_prev,
    "dat_fpxr" AS dat_fpxr,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_is_current" AS _etl_is_current,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
    FROM {{ source('ods', 'produit') }}
    WHERE "_etl_is_current" = TRUE
    {% if is_incremental() %}
    AND "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    