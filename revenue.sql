SELECT 

TRUNC(ts) AS date,
--EXTRACT(HOUR FROM ts) AS HOUR,
--waterfall,
--demand,

demand,
ecpm,
SUM(DECODE(state,'inventory',1)) AS inventory,
SUM(DECODE(state,'waterfallOK',1)) AS waterfallOK,
SUM(DECODE(state,'adAttempt',1)) AS ad_attempts,
SUM(DECODE(state,'adOpportunity',1)) AS ad_opportunities,
SUM(DECODE(state,'impression',1)) AS impressions,
SUM(DECODE(state,'click',1)) AS click,
SUM(DECODE(state,'start',1)) AS start,
SUM(DECODE(state,'complete',1)) AS complete,
(SUM(DECODE(state,'impression',1)) * CAST(ecpm as float) / 1000) AS revenue
--SUM(DECODE(state,'impression',1)) / SUM(DECODE(state,'inventory',1)) AS imps_inv
FROM  video.video_player_logs

--WHERE ts >= getdate() - interval '12 hours' AND placement LIKE '%outfit%' AND s_ver='1.3.2' --limit 100 
WHERE ts>= CURRENT_DATE - 3 --AND demand LIKE 'Yume-App%'  --campaign in ('1367887', '1329135')   --BETWEEN '2016-06-09' AND '2016-06-14'



 --AND os='iOS' --AND osver='7.1.0'--AND waterfall NOT LIKE '%app%'--AND demand LIKE 'Pubmatic-MIX%'



  --EXTRACT(HOUR FROM ts) >= '09'   --AND c_ver is not null
GROUP BY 1,2,3;
--ORDER BY HOUR ASC;
