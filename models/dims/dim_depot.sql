{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'depot',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION DEPOT
============================================================================
Source: prep.tabgco (type_tab = 'DS') + ods.companystatus
Logique: Dépôts/entrepôts avec lien société et analyse comptable
*/

with depots as (
  
  select
    n_tab::integer as depot,
    ndos::integer as ndos,
    inti_tab as nom_depot,
    pays,
    nullif(cod_cli::integer, 0) as cod_cli,
    nullif(cod_fou::integer, 0) as cod_fou
  from {{ source('prep', 'tabgco') }}
  where type_tab = 'DS'
    and n_tab is not null

)

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['d.depot']) }} as depot_key,
  
  d.depot as depot_id,
  d.ndos as societe_id,
  
  -- ==========================================================================
  -- 📝 INFORMATIONS DEPOT
  -- ==========================================================================
  
  d.nom_depot,
  d.pays as pays_depot,
  
  -- ==========================================================================
  -- 🔗 LIENS CLIENT/FOURNISSEUR
  -- ==========================================================================
  
  d.cod_cli as client_interco_id,
  d.cod_fou as fournisseur_interco_id,

  
  -- ==========================================================================
  -- 📅 MÉTADONNÉES
  -- ==========================================================================
  
  current_timestamp as date_creation_dimension

from depots d

where d.depot is not null

order by d.depot