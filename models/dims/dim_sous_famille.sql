{{
  config(
    materialized = 'table',
    schema = 'dims',
    alias = 'sous_famille',
    tags = ['dimension', 'layer:dims']
  )
}}

/*
============================================================================
DIMENSION SOUS-FAMILLE PRODUIT
============================================================================
Source: prep.tabgco (type_tab = 'SF')
Logique: Sous-familles avec lien vers famille
        - Famille = tout sauf 2 derniers chiffres
        - Sous-famille code = 2 derniers chiffres
*/

with sous_familles_raw as (
  
  select
    n_tab::integer as s_famille,
    inti_tab as libelle_sous_famille,
    -- Extraire famille (enlever 2 derniers chiffres)
    (left(n_tab::text, length(n_tab::text) - 2))::integer as famille_id,
    -- Extraire code sous-famille (2 derniers chiffres)
    (right(n_tab::text, 2))::integer as sousfamille_code
  from {{ source('prep', 'tabgco') }}
  where type_tab = 'SF'
    and n_tab is not null
    and length(n_tab::text) > 2  -- Éviter erreurs si code trop court

),

familles as (
  
  select
    famille_key,
    famille_id
  from {{ ref('dim_famille') }}

)

select
  -- ==========================================================================
  -- 🔑 CLÉS
  -- ==========================================================================
  
  {{ dbt_utils.generate_surrogate_key(['sf.s_famille']) }} as sous_famille_key,
  
  sf.s_famille as sous_famille_id,
  
  -- ==========================================================================
  -- 📝 LIBELLÉS
  -- ==========================================================================
  
  sf.libelle_sous_famille,
  
  -- ==========================================================================
  -- 🔗 HIÉRARCHIE
  -- ==========================================================================
  
  sf.famille_id,
  f.famille_key,
  sf.sousfamille_code,
  
  -- ==========================================================================
  -- 📅 MÉTADONNÉES
  -- ==========================================================================
  
  current_timestamp as date_creation_dimension

from sous_familles_raw sf

left join familles f
  on f.famille_id = sf.famille_id

order by sf.s_famille