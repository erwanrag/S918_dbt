{{ config(
    materialized = 'table',
    schema = 'dim',
    alias = 'produit'
) }}

with produit as (

    select *
    from {{ source('prep', 'produit') }}
    where typ_elem in ('AR', 'ARC')
      and statut < 9
      and consigne = false
      and cod_pro <> 1
      and left(refint, 1) <> 'C'
      and groupe in ('BU', 'TR')
      and _etl_is_current = true
),

famille as (
    select
        n_tab::int        as famille,
        inti_tab          as libelle_famille
    from {{ source('prep', 'tabgco') }}
    where type_tab = 'FA'
),

sous_famille as (
    select
        n_tab::int        as s_famille,
        inti_tab          as libelle_sous_famille
    from {{ source('prep', 'tabgco') }}
    where type_tab = 'SF'
)

select
    -- 🔑 clé technique
    {{ dbt_utils.generate_surrogate_key(['p.cod_pro']) }} as produit_key,

    -- 🧠 clés métier
    p.cod_pro,
    p.nom_pro,
    p.refint,
    p.refext,

    -- 🔗 type produit
    te.type_element_key,
    te.type_elem,
    te.type_niveau,

    -- 📦 hiérarchie produit
    p.groupe,
    p.famille,
    f.libelle_famille,
    p.s_famille,
    sf.libelle_sous_famille,

    -- ⚙️ statut
    p.statut,
    case p.statut
        when 0 then 'RAS'
        when 1 then 'Interdit achat'
        when 2 then 'Interdit vente'
        when 8 then 'Interdit achat et vente'
        when 9 then 'À supprimer'
        else 'Inconnu'
    end as statut_libelle,

    -- 🧪 qualité
    p.zta_3 as qualite,
    case p.zta_3
        when 'OE'  then 'Original Equipment'
        when 'OEM' then 'Original Equipment Manufacturer'
        when 'PMQ' then 'Pièce Marché Qualité'
        when 'PMV' then 'Pièce Marché VAR'
        when 'V'   then 'VAR'
        else 'Inconnue'
    end as qualite_libelle,

    -- 🕒 dates
    p.dat_crt,
    p.dat_mod,

    -- 🔄 remplacements
    p.art_remp,

    -- 📊 métadonnées ETL
    p._etl_is_current,
    p._etl_run_id,
    p._prep_loaded_at

from produit p
left join {{ ref('dim_type_element') }} te
    on te.type_elem = p.typ_elem
    and coalesce(te.sous_type, '_NULL') = coalesce(p.sous_type, '_NULL')

left join famille f
    on f.famille = p.famille

left join sous_famille sf
    on sf.s_famille = p.s_famille
