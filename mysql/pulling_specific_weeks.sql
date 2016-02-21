SELECT year(departure), week(departure), count(distinct(departure))
from sched_stops
group by week(departure);

SELECT MAX(start_date)
from trainID;


SELECT year(now()), week(NOW());


SELECT *, week(start_date)
from trainID
WHERE
	week(start_date) = 37 AND
    year(start_date) = 2015;
    
SELECT * from sched_stops as a
WHERE exists
(
SELECT *
from trainID as b
where a.full_id = b.full_id and
	week(b.start_date) = 38 AND
    year(b.start_date) = 2015
) AND
enroute_conf = 0;













