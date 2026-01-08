{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'client',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION CLIENT
============================================================================
Source: prep.client + prep.tabgco
Logique: Version courante uniquement (SCD2 dans PREP)
*/

with client_current as (
  
  select *
  from {{ source('prep', 'client') }}
  where _etl_is_current = true
    and statut < 9  -- Exclure clients à supprimer

),

interco_clients as (
  
  -- Clients intercompany depuis tabgco
  select distinct cod_cli
  from {{ source('prep', 'tabgco') }}
  where type_tab = 'DS'
    and cod_cli is not null
    and cod_cli::text <> '0'
  
  union
  
  -- Clients interco identifiés par pattern nom
  select cod_cli
  from {{ source('prep', 'client') }}
  where _etl_is_current = true
    and (
      nom_cli ilike '%B7 BUS%' 
      or nom_cli ilike '%RETRO%' 
      or nom_cli ilike '%BESSET%'
    )

),

type_element as (
  
  select *
  from {{ ref('dim_type_element') }}
  where type_niveau = 'C'  -- Filtrer uniquement les types liés aux clients

)

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['c.cod_cli']) }} as client_key,
  
  c.cod_cli as client_id,
  
  -- ==========================================================================
  -- 📝 IDENTIFIANTS CLIENT
  -- ==========================================================================
  
  c.nom_cli as nom_client,
  c.pays,
  c.mot_cle as mot_cle_commercial,
  
  -- ==========================================================================
  -- 🔗 TYPE CLIENT
  -- ==========================================================================
  
  te.type_element_key,
  te.type_elem as type_client,
  te.libelle as type_libelle,
  
  -- ==========================================================================
  -- ⚙️ STATUT & CLASSIFICATION
  -- ==========================================================================
  
  c.statut as statut_code,
  case c.statut
    when 0 then 'Actif'
    when 1 then 'Interdit vente'
    when 2 then 'Interdit achat'
    when 8 then 'Interdit vente et achat'
    when 9 then 'À supprimer'
    else 'Inconnu'
  end as statut_libelle,
  
  c.statut = 0 as is_active,
  c.statut = 9 as is_to_delete,
  
  -- ==========================================================================
  -- 🏢 INTERCOMPANY
  -- ==========================================================================
  
  case 
    when ic.cod_cli is not null then true 
    else false 
  end as is_interco,
  
  -- ==========================================================================
  -- 💰 TARIFICATION & DEVISE
  -- ==========================================================================
  
  c.no_tarif as numero_tarif,
  upper(c.famille) as famille_client,
  
  -- ==========================================================================
  -- 📅 DATES MÉTIER
  -- ==========================================================================
  
  c.dat_crt as date_creation,
  c.dat_mod as date_modification,
  
  -- ==========================================================================
  -- 📊 MÉTADONNÉES ETL
  -- ==========================================================================
  
  c._etl_is_current as est_version_courante,
  c._etl_run_id as run_id,
  c._prep_loaded_at as date_chargement_prep,
  current_timestamp as date_creation_dimension

from client_current c

left join type_element te
  on te.type_elem = c.typ_elem

left join interco_clients ic
  on ic.cod_cli = c.cod_cli

where c.cod_cli is not null
  and c.cod_cli::text <> '0'

order by c.cod_cli