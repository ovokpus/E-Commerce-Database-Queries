--#########################################TASK 1################################--
SELECT 
	DATE_PART('week',ps.created_at) AS week_number, 
	p.name AS plan_name,
	COUNT(*) AS plan_name_count
FROM plan_subscriptions AS ps
INNER JOIN plans AS p
ON ps.plan_id = p.id
GROUP BY week_number, plan_name

UNION

SELECT 
	DATE_PART('week',sp.created_at) AS week_number,
	p.name AS plan_name,
	COUNT(*) AS plan_name_count
FROM stripe_plans AS sp
INNER JOIN plans AS p
ON sp.plan_id = p.id
GROUP BY week_number, plan_name
ORDER BY week_number, plan_name;


--#########################################TASK 2################################--
SELECT 
	u.utm_source,
	p.name as plan_name,
	COUNT(*) as count
FROM utms as u
INNER JOIN plan_subscriptions as ps
on u.store_id = ps.store_id
INNER JOIN plans as p
ON p.id = ps.plan_id
GROUP BY u.utm_source, p.name
ORDER BY u.utm_source, p.name;


--#########################################TASK 3################################--
WITH subscription_cte AS (
	SELECT 
		created_at, 
		updated_at, 
		downgraded_at, 
		status, 
		store_id, 
		plan_id
	FROM plan_subscriptions
	UNION
	SELECT 
		created_at, 
		updated_at, 
		downgraded_at, 
		status, 
		store_id, 
		plan_id
	FROM stripe_subscriptions
	WHERE overdue_reason IS NULL
)
SELECT 
	DISTINCT DATE_PART('week', c.updated_at) as week_number,
	COUNT(*) as counts
FROM subscription_cte as c
INNER JOIN stores as s
ON c.store_id = s.id
INNER JOIN plans as p
ON c.plan_id = p.id
WHERE DATE_PART('day', c.downgraded_at) > DATE_PART('day',c.created_at) + p.trial_days
OR (paused_until IS NOT NULL AND status = 2)
GROUP BY week_number
ORDER BY week_number;

--#########################################TASK 4################################--
WITH subscription_cte AS (
	SELECT 
		created_at, 
		downgraded_at, 
		updated_at, 
		(DATE_TRUNC('month', updated_at) + interval '1 month - 1 day')::date as updated_in_month, 
		status, 
		store_id, 
		plan_id
	FROM plan_subscriptions
	UNION
	SELECT 
		created_at,
		downgraded_at, 
		updated_at,
		(DATE_TRUNC('month', updated_at) + interval '1 month - 1 day')::date as updated_in_month,
		status,
		store_id, 
		plan_id
	FROM stripe_subscriptions
	WHERE overdue_reason IS NULL
)
SELECT 
	updated_in_month as month, 
	COUNT(*) AS count
FROM subscription_cte as c
INNER JOIN stores as s
ON c.store_id = s.id
INNER JOIN plans as p
ON c.plan_id = p.id
WHERE (DATE_PART('day', c.downgraded_at) > DATE_PART('day',c.created_at) + p.trial_days
OR (paused_until IS NOT NULL AND status = 2))
AND EXTRACT(YEAR FROM c.updated_at)=2020
GROUP BY c.updated_in_month;

--#########################################TASK 5################################--
WITH subscription_cte AS (
	SELECT 
		id, 
		created_at,
		downgraded_at,
		status, 
		store_id,
		plan_id
	FROM plan_subscriptions
	UNION
	SELECT 
		id, 
		created_at,
		downgraded_at,
		status,
		store_id,
		plan_id
	FROM stripe_subscriptions
)
SELECT 
	store_id,
	COUNT(*) as subscription_count, 
	AVG(c.downgraded_at - c.created_at + interval '1' day * p.trial_days) as average_retention
FROM subscription_cte as c
INNER JOIN stores as s
ON c.store_id = s.id
INNER JOIN plans as p
ON c.plan_id = p.id
WHERE (DATE_PART('day', c.downgraded_at) > DATE_PART('day',c.created_at) + p.trial_days AND NOT c.status = 2)
OR status = 2
GROUP BY store_id;
--#########################################TASK 6################################--
SELECT 
	store_id, 
	COUNT(*) AS import_count, 
	(MIN(pushed_at)-MIN(created_at)) AS time_to_first_import
FROM imports
WHERE pushed_at IS NOT NULL
GROUP BY store_id;

--#########################################TASK 7################################--
WITH retention_cte AS(
	WITH subscription_cte AS (
		SELECT id, created_at, downgraded_at, status, store_id, plan_id
		FROM plan_subscriptions
		UNION
		SELECT id, created_at, downgraded_at, status, store_id, plan_id
		FROM stripe_subscriptions
	)
	SELECT 
		store_id,
		COUNT(*) as subscription_count, 
		AVG(c.downgraded_at - c.created_at + interval '1' day * p.trial_days) as average_retention
	FROM subscription_cte as c
	INNER JOIN stores as s
	ON c.store_id = s.id
	INNER JOIN plans as p
	ON c.plan_id = p.id
	WHERE (DATE_PART('day', c.downgraded_at) > DATE_PART('day',c.created_at) + p.trial_days AND NOT c.status = 2)
	OR status = 2
	GROUP BY store_id
),
import_cte AS(
	SELECT store_id, COUNT(*) AS import_count, (MIN(pushed_at)-MIN(created_at)) AS time_to_first_import
	FROM imports
	WHERE pushed_at IS NOT NULL
	GROUP BY store_id
)
SELECT b.import_count, COUNT(b.store_id) AS store_count, AVG(b.time_to_first_import) AS average_time_to_first_import, a.average_retention
FROM import_cte as b
INNER JOIN retention_cte as a
ON a.store_id = b.store_id
GROUP BY b.import_count, b.time_to_first_import, a.average_retention;