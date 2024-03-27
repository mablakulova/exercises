WITH cte
AS (
	SELECT DISTINCT employeemanage.employee_id employee_id,
		employee.full_name employee_fullname,
		position.id position_id,
		COALESCE(positiontranslate.translate_text::CHARACTER VARYING, position.short_name) positon_name,
		TO_CHAR(employeemanage.start_on, 'DD.MM.YYYY') starton,
		TO_CHAR(employeelog.event_on, 'DD.MM.YYYY') eventon,
		employeelog.employee_turnstile_log_type_id action_type,
		TRIM(BOTH ' ' FROM TO_CHAR(employeelog.event_on, 'Day')) AS week_day,
		DATE_TRUNC('minute', employeelog.event_at) event_at,
		scheduledayhour.begin_at::interval begin_schedule_time,
		scheduledayhour.end_at::interval end_schedule_time,
		row_number() OVER (ORDER BY employeelog.event_at) AS rn
	FROM hrm.sys_employee_turnstile_log employeelog
	LEFT JOIN hrm.sys_employee_manage employeemanage
		ON employeelog.employee_id = employeemanage.employee_id
	LEFT JOIN public.hl_employee employee
		ON employeemanage.employee_id = employee.id
	LEFT JOIN public.hl_position position
		ON employeemanage.position_id = position.id
	LEFT JOIN hrm.info_work_schedule schedule
		ON schedule.id = employeemanage.work_schedule_id
	LEFT JOIN hrm.info_work_schedule_day_hour scheduledayhour
		ON scheduledayhour.owner_id = schedule.id
	LEFT JOIN public.hl_position_translate positiontranslate
		ON positiontranslate.owner_id = position.id
	WHERE employee.id = 20 AND employeelog.event_on = '2024-02-16' AND scheduledayhour.day_number = EXTRACT(ISODOW FROM employeelog.event_on::TIMESTAMP)
	),
cte2
AS (
	SELECT a.employee_id,
		a.employee_fullname,
		a.position_id,
		a.positon_name,
		a.starton,
		a.eventon,
		a.week_day,
		CASE 
			WHEN CASE 
					WHEN a.begin_schedule_time >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END >= a.end_schedule_time
				THEN a.end_schedule_time
			ELSE CASE 
					WHEN a.begin_schedule_time >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END
			END AS enterschedule,
		CASE 
			WHEN a.end_schedule_time <= TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval
				THEN a.end_schedule_time
			ELSE TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval
			END AS exitschedule,
		CASE 
			WHEN entertime.event_at IS NULL
				THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
			ELSE entertime.event_at
			END AS entertime,
		CASE 
			WHEN exittime.event_at IS NULL
				THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS exittime
	FROM cte a
	LEFT JOIN (
		SELECT employee_id,
			event_at,
			rn
		FROM cte
		WHERE action_type = 1
		) AS entertime
		ON a.employee_id = entertime.employee_id AND a.rn = entertime.rn
	LEFT JOIN (
		SELECT employee_id,
			event_at,
			rn
		FROM cte
		WHERE action_type = 2
		) AS exittime
		ON a.employee_id = exittime.employee_id AND a.rn = exittime.rn - 1
	ORDER BY CASE 
			WHEN entertime.event_at IS NULL
				THEN LAG(exittime.event_at) OVER (
						ORDER BY exittime.rn
						)
			ELSE entertime.event_at
			END
	),
cte3
AS (
	SELECT *,
		(DATE_PART('day', exittime - entertime) * 24 + DATE_PART('hour', exittime - entertime) * 60 + DATE_PART('minute', exittime - entertime)) periodMinute,
		(DATE_PART('hour', exitschedule - enterschedule) * 60 + DATE_PART('minute', exitschedule - enterschedule)) periodMinuteSchedule
	FROM cte2
	WHERE (entertime IS NOT NULL AND exittime IS NOT NULL) AND (entertime <> exittime)
	)
SELECT *
FROM cte3

select * 
, event_At - INTERVAL '5 hours' as wordday
FROM hrm.sys_employee_turnstile_log 
where event_on <> (event_At - INTERVAL '5 hours')::date