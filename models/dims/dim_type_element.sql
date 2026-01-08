{{ config(
    materialized = 'table',
    schema = 'dims',
    alias = 'type_element',
    tags = ['dimension', 'layer:dims']
) }}

select
    -- 🔑 clé technique
    {{ dbt_utils.generate_surrogate_key([
        'typ_fich',
        'typ_elem',
        'sous_type'
    ]) }} as type_element_key,

    -- 🧠 clés métier NORMALISÉES
    typ_fich  as type_niveau,   -- P / C / F / Z / OP / FL
    CASE WHEN typ_fich = 'P' THEN 'PRODUIT'
         WHEN typ_fich = 'C' THEN 'CLIENT'
         WHEN typ_fich = 'F' THEN 'FOURNISSEUR'
         ELSE 'INCONNU'
    END as type_fiche_libelle,
    typ_elem  as type_elem,     -- AR / DF / CLI / FOU / ...
    sous_type,

    -- 📘 sémantique
    lib_elem  as libelle,

    -- ⚙️ règles métier
    stock     as autorise_stock,
    commande  as autorise_commande,
    nmc_pro   as autorise_vente,
    nmc_com   as autorise_achat,
    stat      as statut

from {{ source('prep', 'typelem') }}
where typ_elem is not null AND typ_fich IN ('P', 'C', 'F')
order by typ_fich, typ_elem, sous_type
