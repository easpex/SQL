SELECT 
traffic_channel,
sum(impressions)
--date(datereceived) AS date,
--count(extplacementid) AS count
FROM video.vw_ias_traffic_scope--ias_traffic_scope 
where date = '2016-08-23'
group by 1;



select * from ias_traffic_scope 
where date(datereceived) = CURRENT_DATE-3 AND extadvertiserid='videoPixelSeat' limit 100



SELECT adultsc,
       extplacementid
FROM video.ias_traffic_scope 
WHERE adultsc != 'N/A' LIMIT 100


group by 1,2
