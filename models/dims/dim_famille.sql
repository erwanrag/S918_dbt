{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'famille',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION FAMILLE PRODUIT
============================================================================
Source: prep.tabgco (type_tab = 'FA')
Logique: Familles de produits (premier niveau hiérarchie)
*/

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['n_tab']) }} as famille_key,
  
  n_tab::integer as famille_id,
  
  -- ==========================================================================
  -- 📝 LIBELLÉ
  -- ==========================================================================
  
  inti_tab as libelle_famille,
  
  -- ==========================================================================
  -- 📅 MÉTADONNÉES
  -- ==========================================================================
  
  current_timestamp as date_creation_dimension

from {{ source('prep', 'tabgco') }}

where type_tab = 'FA'
  and n_tab is not null

order by n_tab::integer