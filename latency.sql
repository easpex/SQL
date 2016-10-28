
SELECT DATE,
       platform,
       waterfall,
       c_ver,
       demand,
       time_bucket,
       "ad_attempts->ad_opportunity(s)",
       "ad_opportunity->impression(s)",
       adAttemptcount,
       adOpportunitycount,
       impressioncount,
       inventorycount,
       SUM(adAttemptcount) OVER (partition by DATE,platform,waterfall,c_ver,demand ORDER BY time_bucket ROWS UNBOUNDED PRECEDING) AdAttemptAcc,
       SUM(impressioncount) OVER (partition by DATE,platform,waterfall,c_ver,demand ORDER BY time_bucket ROWS UNBOUNDED PRECEDING) ImpAcc,
       SUM(adAttemptcount) OVER (partition by DATE,platform,waterfall,c_ver,demand ORDER BY time_bucket ROWS 5 PRECEDING) AdAttemptAcc05s,
       SUM(impressioncount) OVER (partition by DATE,platform,waterfall,c_ver,demand ORDER BY time_bucket ROWS 5 PRECEDING) ImpAcc05s,
       SUM(loss) OVER (partition by DATE,platform,waterfall,c_ver,demand,time_bucket) as loss
FROM (SELECT DATE,
             platform,
             waterfall,
             c_ver,
             demand,
             ROUND((CAST("inventory->ad_attempt(ms)" AS float8)) / 1000,1) time_bucket,
             SUM(CAST("ad_attempts->ad_opportunity(ms)" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)",
             SUM(CAST("ad_opportunity->impression(ms)" AS float8)) / 1000 AS "ad_opportunity->impression(s)",
             -- 				MIN(CAST("ad_attempts->ad_opportunity(ms)" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)min",
             -- 				MIN(CAST("ad_opportunity->impression(ms)" AS float8)) / 1000 AS "ad_opportunity->impression(s)min",
             -- 				MAX(CAST("ad_attempts->ad_opportunity(ms)" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)max",
             -- 				MAX(CAST("ad_opportunity->impression(ms)" AS float8)) / 1000 AS "ad_opportunity->impression(s)max",
             -- 				AVG(CAST("ad_attempts->ad_opportunity(ms)0.05" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)0.05",
             -- 				AVG(CAST("ad_attempts->ad_opportunity(ms)0.50" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)0.50",
             -- 				AVG(CAST("ad_attempts->ad_opportunity(ms)0.95" AS float8)) / 1000 AS "ad_attempts->ad_opportunity(s)0.95",
             -- 				AVG(CAST("ad_opportunity->impression(ms)0.05" AS float8)) / 1000 AS "ad_opportunity->impression(s)0.05",
             -- 				AVG(CAST("ad_opportunity->impression(ms)0.50" AS float8)) / 1000 AS "ad_opportunity->impression(s)0.50",
             -- 				AVG(CAST("ad_opportunity->impression(ms)0.95" AS float8)) / 1000 AS "ad_opportunity->impression(s)0.95",
             -- 				STDDEV_POP(CAST("ad_attempts->ad_opportunity(ms)" AS float8)/1000) AS "ad_attempts->ad_opportunity(s)STD",
             -- 				STDDEV_POP(CAST("ad_opportunity->impression(ms)" AS float8)/1000) AS "ad_opportunity->impression(s)STD",
             SUM(adAttemptcount) AS adAttemptcount,
             SUM(adOpportunitycount) AS adOpportunitycount,
             SUM(impressioncount) AS impressioncount,
             SUM(inventorycount) AS inventorycount,
             SUM(CASE when next_adattempt_time is null and next_impression_time is null and passback_time is null then 1 else null end) loss
      FROM (SELECT DATE,
                   demand,
                   sessionid,
                   waterfall,
                   platform,
                   client_ip,
                   play_count,
                   c_ver,
                   adAttempt_time 
                   		  - (CASE when LAG(complete_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall ORDER BY adAttempt_time) is null then max(inventory_time) over (partition by sessionid)
                   		    else LAG(complete_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall ORDER BY adAttempt_time) end) 
                   		  - (case when play_count>1 and mod(play_count,3)=1 then 20000 else 0 end) --reducing the content time every 3 pre-rolls
                   		   --nvl(LAG(play_count_max_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall,play_count ORDER BY adAttempt_time),0) 
                   AS "inventory->ad_attempt(ms)",
                   nvl(LAG(play_count_max_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall,play_count ORDER BY adAttempt_time),0),
                   "ad_attempts->ad_opportunity(ms)",
                   --CASE
                    --- WHEN platform <> 'desktop' THEN "ad_attempts->ad_opportunity(ms)"
                  --   ELSE adAttempt_time - nvl (LAG(adAttempt_time) IGNORE NULLS OVER (PARTITION BY DATE,sessionid,waterfall,play_count,platform,client_ip,c_ver ORDER BY adAttempt_time),0)
                   --problem if it's the first demand, what to do here?
                   --END AS "ad_attempts->ad_opportunity(ms)",
                   "ad_opportunity->impression(ms)",
                   inventorycount,
                   adAttemptcount,
                   adOpportunitycount,
                   impressioncount,
                   --        PERCENTILE_DISC(0.05) within group (order by "ad_attempts->ad_opportunity(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_attempts->ad_opportunity(ms)0.05",
                   --        PERCENTILE_DISC(0.50) within group (order by "ad_attempts->ad_opportunity(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_attempts->ad_opportunity(ms)0.50",
                   --        PERCENTILE_DISC(0.95) within group (order by "ad_attempts->ad_opportunity(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_attempts->ad_opportunity(ms)0.95",
                   --        PERCENTILE_DISC(0.05) within group (order by "ad_opportunity->impression(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_opportunity->impression(ms)0.05",
                   --        PERCENTILE_DISC(0.50) within group (order by "ad_opportunity->impression(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_opportunity->impression(ms)0.50",
                   --        PERCENTILE_DISC(0.95) within group (order by "ad_opportunity->impression(ms)") over(partition by DATE,platform,demand,waterfall,play_count,c_ver) "ad_opportunity->impression(ms)0.95",
                   CASE when LAG(complete_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall ORDER BY adAttempt_time) is null then max(inventory_time) over (partition by sessionid)
                   		  else LAG(complete_time) IGNORE NULLS OVER (PARTITION BY sessionid,waterfall ORDER BY adAttempt_time) end
                   		  as last_completed_time,
                   LAG(adAttempt_time) IGNORE NULLS OVER (PARTITION BY DATE,sessionid,waterfall,play_count,platform,client_ip,c_ver ORDER BY adAttempt_time) last_adattempt_time,
                   LEAD(adAttempt_time) IGNORE NULLS OVER (PARTITION BY DATE,sessionid,waterfall,platform,client_ip,c_ver ORDER BY adAttempt_time) next_adattempt_time,
                   LEAD(impression_time) IGNORE NULLS OVER (PARTITION BY DATE,sessionid,waterfall,platform,client_ip,c_ver ORDER BY impression_time) next_impression_time,
                   passback_time,
                   adAttempt_time,
                   play_count_max_time
            FROM (SELECT TRUNC(ts) AS DATE,
                         demand,
                         sessionid,
                         waterfall,
                         platform,
                         client_ip,
                         play_count,
                         c_ver,
                         --  elapsed_time,
                         /*
       MIN(DECODE(state,'impression',ts,null)) AS impressions,
       MIN(DECODE(state,'click',ts,null)) AS clicks,
       MIN(DECODE(state,'adAttempt',ts,null)) AS ad_attempt,
       MIN(DECODE(state,'adOpportunity',ts,null)) AS ad_opportunity,
       MIN(DECODE(state,'adView',ts,null)) AS ad_view,
       MIN(DECODE(state,'start',ts,null)) AS start,
       MIN(DECODE(state,'firstQuartile',ts,null)) AS first_quartile,
       MIN(DECODE(state,'midpoint',ts,null)) AS midpoint,
       MIN(DECODE(state,'thirdQuartile',ts,null)) AS third_quartile,
       MIN(DECODE(state,'complete',ts,null)) AS complete,
       MIN(DECODE(state,'passback',ts,null)) AS passback,
       SUM(CASE WHEN c_ver LIKE '1.%' THEN DECODE(state,'inventory',1) ELSE DECODE(state,'impression',1) END) AS inventory,
       SUM(CASE WHEN c_ver LIKE '1.%' THEN DECODE(state,'impression',1) ELSE DECODE(state,'adView',1) END) AS impressions,
       SUM(CASE WHEN c_ver LIKE '1.%' THEN CAST(ecpm*DECODE(state,'impression',1) AS float8) / 1000 ELSE CAST(ecpm*DECODE(state,'adView',1) AS float8) / 1000 END) AS revenue
       */ 
       									 1 AS inventorycount,
                         SUM(DECODE(state,'adAttempt',1)) AS adAttemptcount,
                         SUM(DECODE(state,'adOpportunity',1)) AS adOpportunitycount,
                         SUM(CASE WHEN c_ver LIKE '1.%' THEN DECODE(state,'impression',1) ELSE DECODE(state,'adView',1) END) AS impressioncount,
                         MAX(DECODE(state,'complete',elapsed_time,NULL)) complete_time,
                         MAX(DECODE(state,'inventory',elapsed_time,NULL)) inventory_time,
                         MAX(DECODE(state,'adAttempt',elapsed_time,NULL)) adAttempt_time,
                         MAX(DECODE(state,'impression',elapsed_time,NULL)) impression_time,
                         MAX(DECODE(state,'passback',elapsed_time,NULL)) passback_time,
                         MIN(DECODE(state,'adOpportunity',elapsed_time,NULL)) ad_opportunity_time,
                         MAX(elapsed_time) play_count_max_time,
                         MIN(DECODE(state,'adOpportunity',elapsed_time,NULL)) -MIN(DECODE(state,'adAttempt',elapsed_time,NULL)) AS "ad_attempts->ad_opportunity(ms)",
                         MIN(CASE WHEN c_ver LIKE '1.%' THEN DECODE(state,'impression',elapsed_time,NULL) ELSE DECODE(state,'adView',elapsed_time,NULL) END) -MIN(DECODE(state,'adOpportunity',elapsed_time,NULL)) AS "ad_opportunity->impression(ms)"
                  FROM video.video_player_logs
                  --where ts between trunc(current_date-1) and trunc(current_date)  AND demand in ('Adkarma-Small-EN-WL-SPS-4', 'Adkarma-Small-ROW-WL-SPS-4')
                 WHERE  ts between '2016-06-10' AND '2016-06-12' AND demand in ('Adkarma-Small-EN-WL-SPS-4', 'Adkarma-Small-ROW-WL-SPS-4', 'DivisionD-Small-EN-3')
                  
                  AND elapsed_time>=0
                  --and lower(publisher)<>'mobilecore'
                  --and demand='Vdopia-App-US-TalkingBenTheDog-4'
                  --AND   c_ver = '1.0.7'
                  --and play_count=1
                  --and ts between '2015-11-25' and '2015-11-26'
                  --and sessionid='0b6176f1-773e-418a-bded-ffef8478cc15'
                  GROUP BY TRUNC(ts),
                           platform,
                           demand,
                           sessionid,
                           waterfall,
                           client_ip,
                           play_count,
                           c_ver
                           )
            --WHERE demand = 'Vdopia-App-US-TalkingBenTheDog-4'
            )
      Where "inventory->ad_attempt(ms)">=0   AND   demand <> '[DEMAND]'   AND   play_count IS NOT NULL
      GROUP BY 1,
               2,
               3,
               4,
               5,
               6
               ) 
             
