SELECT 
--TRUNC(ts) AS date,
--EXTRACT(HOUR FROM ts) AS HOUR,
latitude,
longitude,
--CAST(paid_price AS FLOAT),
SUM(DECODE(state,'inventory',1)) AS inventory,
SUM(DECODE(state,'waterfallOK',1)) AS waterfallOK,
SUM(DECODE(state,'adAttempt',1)) AS ad_attempts,
SUM(DECODE(state,'adServe',1)) AS adServe,
SUM(DECODE(state,'adOpportunity',1)) AS ad_opportunities,
SUM(DECODE(state,'impression',1)) AS impressions,
SUM(DECODE(state,'start',1)) AS start,
SUM(DECODE(state,'complete',1)) AS complete
--(SUM(DECODE(state,'impression',1)) *  CAST(ecpm as float) / 1000) AS revenue
--SUM(DECODE(state,'impression',1)) / SUM(DECODE(state,'inventory',1)) AS imps_inv
FROM  video.video_player_logs

--WHERE ts >= getdate() - interval '12 hours' AND placement LIKE '%outfit%' AND s_ver='1.3.2' --limit 100 
WHERE ts>= CURRENT_DATE-1  AND  waterfall='cleanMaster_app_dc_us' --AND demand='Static-LKQD-Sungy-IAS-0'
  --AND demand='Spot-CleanMaster-App-ROW-VPAID-JS-6'
 --AND os='iOS' --AND osver='7.1.0'--AND waterfall NOT LIKE '%app%'--AND demand LIKE 'Pubmatic-MIX%'
GROUP BY 1,2


  --EXTRACT(HOUR FROM ts) >= '09'   --AND c_ver is not null
--HAVING SUM(DECODE(state,'inventory',1)) > 3
--ORDER BY HOUR ASC;
