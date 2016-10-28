SELECT 
--TRUNC(ts) AS date,
--EXTRACT(HOUR FROM ts) AS HOUR,
placement,
demand,
SUM(DECODE(state,'inventory',1)) AS inventory,
SUM(DECODE(state,'waterfallOK',1)) AS waterfallOK,
SUM(DECODE(state,'adAttempt',1)) AS ad_attempts,
SUM(DECODE(state,'adOpportunity',1)) AS ad_opportunities,
SUM(DECODE(state,'impression',1)) AS impressions,
SUM(DECODE(state,'start',1)) AS start,
SUM(DECODE(state,'complete',1)) AS complete,
SUM(DECODE(state,'firstQuartile',1)) AS first_quartile     
FROM  video.video_player_logs 
--WHERE ts >= CURRENT_DATE AND publisher='tango' --AND s_ver in ('1.3.0', '1.2.0', '1.3.2') 
--WHERE ts >= getdate() - interval '1 days' AND waterfall='of7_app_dc_in_android' AND s_ver is not NULL 
WHERE ts >= CURRENT_DATE AND waterfall='mopubVastServer_app_mp_us'
GROUP BY 1,2;
--ORDER BY ad_attempts DESC;


