{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'marque',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION MARQUE
============================================================================
Source: ods.crn (table concurrents/marques)
Logique: Marques de produits concurrents
*/

with marques as (
  
  select distinct
    cod_crn::integer as marque_code,
    nom_crn as marque_libelle
  from {{ source('ods', 'crn') }}
  where _etl_is_current = true
    and nom_crn is not null
    and trim(nom_crn) <> ''

)

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['marque_code']) }} as marque_key,
  
  marque_code,
  
  -- ==========================================================================
  -- 📝 LIBELLÉ
  -- ==========================================================================
  
  marque_libelle,
  
  -- ==========================================================================
  -- 📅 MÉTADONNÉES
  -- ==========================================================================
  
  current_timestamp as date_creation_dimension

from marques

order by marque_code