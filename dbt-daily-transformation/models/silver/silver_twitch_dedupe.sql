{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ingested_at_utc','twitch_game_id'],
    liquid_clustered_by=['ingested_at_utc', 'twitch_game_id'],
    file_format='delta'
) }}

WITH silver_twitch_dedupe AS (
    SELECT * FROM {{ ref('stg_twitch') }}
    {% if is_incremental() %}

      WHERE ingested_at_utc > (SELECT MAX(ingested_at_utc) FROM {{ this }})
    {% endif %}
),

deduplicado AS (
    SELECT 
        twitch_game_id,
        game_name,
        ingested_at_utc,
        viewer_count,
        stream_id
    FROM workspace.default_staging.stg_twitch
QUALIFY row_number() OVER (
    PARTITION BY stream_id, ingested_at_utc
    ORDER BY ingested_at_utc DESC
) = 1
)

SELECT
    twitch_game_id,
    game_name,
    ingested_at_utc,
    SUM(viewer_count) as viewer_count
FROM deduplicado
GROUP BY 
    twitch_game_id,
    game_name,
    ingested_at_utc