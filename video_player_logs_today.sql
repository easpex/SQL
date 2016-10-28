SELECT 
TRUNC(ts) AS date,
--EXTRACT(HOUR FROM ts) AS HOUR,
--waterfall,
--demand,
--waterfall,
waterfall,
--countrycode,
SUM(DECODE(state,'inventory',1)) AS inventory,
SUM(DECODE(state,'waterfallOK',1)) AS waterfallOK,
SUM(DECODE(state,'adAttempt',1)) AS ad_attempts,
SUM(DECODE(state,'adOpportunity',1)) AS ad_opportunities,
SUM(DECODE(state,'impression',1)) AS impressions,
SUM(DECODE(state,'start',1)) AS start,
SUM(DECODE(state,'firstQuartile',1)) AS firstQuartile,
SUM(DECODE(state,'midpoint',1)) AS midpoint,
SUM(DECODE(state,'thirdQuartile',1)) AS thirdQuartile,
SUM(DECODE(state,'complete',1)) AS complete,
SUM(DECODE(state,'ready',1)) AS ready,
SUM(DECODE(state,'pause',1)) AS pause,
SUM(DECODE(state,'resume',1)) AS resume,
SUM(DECODE(state,'passback',1)) AS passback,
SUM(DECODE(state,'passbackClicked',1)) AS passbackClicked,
SUM(DECODE(state,'click',1)) AS click,
SUM(DECODE(state,'interaction',1)) AS interaction,
SUM(DECODE(state,'contentStart',1)) AS contentStart,
SUM(DECODE(state,'contentComplete',1)) AS contentComplete,
CAST(SUM(DECODE(state,'impression',1)) AS FLOAT) / CAST(SUM(DECODE(state,'inventory',1)) AS FLOAT) AS ratio,
CAST(SUM(DECODE(state,'impression',1)) AS FLOAT) / CAST(SUM(DECODE(state,'adAttempt',1)) AS FLOAT) AS fill,
CAST(SUM(DECODE(state,'complete',1)) AS FLOAT) / CAST(SUM(DECODE(state,'start',1)) AS FLOAT) AS completion

--SUM(DECODE(state,'impression',1)) / SUM(DECODE(state,'inventory',1)) AS imps_inv
FROM  video.video_player_logs

--WHERE ts >= getdate() - interval '12 hours' AND placement LIKE '%outfit%' AND s_ver='1.3.2' --limit 100 
WHERE ts >= CURRENT_DATE - 7  AND waterfall in('of7_app_dc_in_android', 'of7_app_dc_row_android')	 --EXTRACT(HOUR FROM ts) >= '09'   --AND c_ver is not null
GROUP BY 1,2;
--ORDER BY HOUR ASC;
