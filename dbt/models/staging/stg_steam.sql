{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['ingested_at_utc','twitch_game_id'],
    liquid_clustered_by=['ingested_at_utc'],
    file_format='delta'
) }}

with
steam as (
        select * from {{ source("bronze_data", "bronze_steam") }}
        {% if is_incremental() %}
            where ingestion_timestamp_utc > (select max(ingested_at_utc) from {{ this }})
        {% endif %}
    )

select 
    -- IDs e Chaves
    id as steam_game_id,
    jogo as game_name,

    -- Métricas
    qtd_jogadores as peak_players,

    -- Datas
    to_timestamp(ingestion_timestamp_utc) as ingested_at_utc

from steam