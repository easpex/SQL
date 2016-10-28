SELECT 
--TRUNC(ts) AS date,
--EXTRACT(HOUR FROM ts) AS HOUR,
waterfall,
category,
COUNT(category)

FROM  video.video_errors 
--WHERE ts >= getdate() - interval '12 hours' AND placement LIKE '%outfit%' AND s_ver='1.3.2' --limit 100 
WHERE ts >= CURRENT_DATE AND waterfall in ('cleanMaster_app_dc_us', 'cleanMasterDesktop_app_dc_us')
GROUP BY 1,2;
--ORDER BY ad_attempts DESC;
