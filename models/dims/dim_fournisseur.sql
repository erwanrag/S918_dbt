{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'fournisseur',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION FOURNISSEUR
============================================================================
Source: prep.fournis + prep.tabgco
Logique: Version courante uniquement (SCD2 dans PREP)
*/

with fournisseur_current as (
  
  select *
  from {{ source('prep', 'fournis') }}
  where _etl_is_current = true
    and statut < 9  -- Exclure fournisseurs à supprimer
),

interco_fournisseurs as (
  
  -- Fournisseurs intercompany depuis tabgco
  select distinct cod_fou
  from {{ source('prep', 'tabgco') }}
  where type_tab = 'DS'
    and cod_fou is not null
    and cod_fou::text <> '0'

),

type_element as (
  
  select *
  from {{ ref('dim_type_element') }}
  where type_niveau = 'F'  -- Filtrer uniquement les types liés aux fournisseurs

)

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['f.cod_fou']) }} as fournisseur_key,
  
  f.cod_fou as fournisseur_id,
  
  -- ==========================================================================
  -- 📝 IDENTIFIANTS FOURNISSEUR
  -- ==========================================================================
  
  f.nom_fou as nom_fournisseur,
  f.pays,
  f.mot_cle as mot_cle_commercial,
  f.acheteur as code_acheteur,
  
  -- ==========================================================================
  -- 🔗 TYPE FOURNISSEUR
  -- ==========================================================================
  
  te.type_element_key,
  te.type_elem as type_fournisseur,
  te.libelle as type_libelle,
  
  -- ==========================================================================
  -- ⚙️ STATUT
  -- ==========================================================================
  
  f.statut as statut_code,
  case f.statut
    when 0 then 'Actif'
    when 1 then 'Interdit achat'
    when 2 then 'Interdit vente'
    when 8 then 'Interdit achat et vente'
    when 9 then 'À supprimer'
    else 'Inconnu'
  end as statut_libelle,
  
  f.statut = 0 as is_active,
  f.statut = 9 as is_to_delete,
  
  -- ==========================================================================
  -- 🏢 INTERCOMPANY
  -- ==========================================================================
  
  case 
    when if_.cod_fou is not null then true 
    else false 
  end as is_interco,
  
  -- ==========================================================================
  -- 📦 DÉLAI DE LIVRAISON
  -- ==========================================================================
  
  -- Extraire premier élément de del_liv (peut être semicolon-separated)
  case 
    when f.del_liv_1 is not null and f.del_liv_1::text <> '' then
      f.del_liv_1::integer
    else null
  end as delai_livraison_jours,
  
  -- ==========================================================================
  -- 📅 DATES MÉTIER
  -- ==========================================================================
  
  f.dat_crt as date_creation,
  f.dat_mod as date_modification,
  
  -- ==========================================================================
  -- 📊 MÉTADONNÉES ETL
  -- ==========================================================================
  
  f._etl_is_current as est_version_courante,
  f._etl_run_id as run_id,
  f._prep_loaded_at as date_chargement_prep,
  current_timestamp as date_creation_dimension

from fournisseur_current f

left join type_element te
  on te.type_elem = f.typ_elem

left join interco_fournisseurs if_
  on if_.cod_fou = f.cod_fou

where f.cod_fou is not null
  and f.cod_fou::text <> '0'

order by f.cod_fou