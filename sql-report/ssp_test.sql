WITH cte
AS (
	SELECT 
	    DISTINCT employeemanage.employee_id employee_id,
		person.full_name employee_name,
		position.id position_id,
		COALESCE(positiontranslate.translate_text::CHARACTER VARYING, position.short_name) positon_name,
		TO_CHAR(employeemanage.start_on, 'DD.MM.YYYY') starton,
		--TO_CHAR((employeelog.event_At - INTERVAL '5 hours')::DATE, 'DD.MM.YYYY') eventon,
		(employeelog.event_At - INTERVAL '5 hours')::DATE eventon,
		employeelog.employee_turnstile_log_type_id action_type,
		TRIM(BOTH ' ' FROM TO_CHAR((employeelog.event_At - INTERVAL '5 hours')::DATE, 'Day')) AS week_day,
		DATE_TRUNC('minute', employeelog.event_at) event_at,
		COALESCE(scheduledayhour.begin_at::interval, '08:00'::interval) AS begin_schedule_time,
		COALESCE(scheduledayhour.end_at::interval, '18:00'::interval) AS end_schedule_time,
		row_number() OVER (ORDER BY employeelog.event_at) AS rn 
    FROM 
	    hrm.sys_employee_turnstile_log employeelog 
    LEFT JOIN 
	    hrm.sys_employee_manage employeemanage ON employeelog.employee_id = employeemanage.employee_id 
    LEFT JOIN 
	    hrm.hl_employee employee ON employeemanage.employee_id = employee.id 
    LEFT JOIN 
	    public.hl_person person ON employee.person_id = person.id
    LEFT JOIN 
	    public.info_position position ON employeemanage.position_id = position.id 
    LEFT JOIN 
	    hrm.info_work_schedule schedule ON schedule.id = employeemanage.work_schedule_id 
    LEFT JOIN 
	    hrm.info_work_schedule_day_hour scheduledayhour ON scheduledayhour.owner_id = schedule.id AND 
        scheduledayhour.day_number = EXTRACT(ISODOW FROM (employeelog.event_at - INTERVAL '5 hours')::DATE::TIMESTAMP)
    LEFT JOIN 
	    public.info_position_translate positiontranslate ON positiontranslate.owner_id = position.id 
    WHERE 
	    employee.id = 47 AND (employeelog.event_at - INTERVAL '5 hours')::DATE = '2024-05-01' AND 
        employee.organization_id = 1
	),
cte2
AS (
	SELECT a.employee_id,
		a.employee_name,
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
				OR TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval <= '05:00'::interval
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
			   --THEN COALESCE(LEAD(entertime.event_at) OVER (ORDER BY entertime.rn), DATE_TRUNC('minute', CURRENT_TIMESTAMP) AT TIME ZONE 'UTC-5')
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
				THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
			ELSE entertime.event_at
			END
	),
cte3
AS (
    SELECT 
	     employee_id,
         MIN(entertime) AS first_entertime,
         MAX(exittime) AS last_exittime,
         MIN(enterschedule) AS first_enterschedule,
         MAX(exitschedule) AS last_exitschedule,
	     COUNT(entertime) AS enter_count,
	     COUNT(exittime) AS exit_count
    FROM 
	    cte2
	WHERE 
       (entertime IS NOT NULL AND exittime IS NOT NULL) AND 
	   (entertime <> exittime)
	GROUP BY 
	   employee_id
  ),
cte4
AS (
    SELECT 
        cte2.*,
	    cte3.first_entertime,
	    cte3.last_exittime,
	    cte3.first_enterschedule, 
	    cte3.last_exitschedule,
	    cte3.enter_count,
	    cte3.exit_count,
	    (DATE_PART('day', cte3.last_exittime - cte3.first_entertime) * 24 + DATE_PART('hour', cte3.last_exittime - cte3.first_entertime) * 60 + 
         DATE_PART('minute', cte3.last_exittime - cte3.first_entertime)) AS total_time,
        (DATE_PART('day', cte2.exittime - cte2.entertime) * 24 + DATE_PART('hour', cte2.exittime - cte2.entertime) * 60 + 
         DATE_PART('minute', cte2.exittime - cte2.entertime)) AS periodMinute,
        (DATE_PART('day', cte2.exitschedule - cte2.enterschedule) * 24 + DATE_PART('hour', cte2.exitschedule - cte2.enterschedule) * 60 + 
         DATE_PART('minute', cte2.exitschedule - cte2.enterschedule)) AS periodMinuteSchedule
    FROM
        cte2
    JOIN 
        cte3 ON cte2.employee_id = cte3.employee_id
    WHERE 
       (cte2.entertime IS NOT NULL AND cte2.exittime IS NOT NULL)
       AND (cte2.entertime <> cte2.exittime)
   )
SELECT
    employee_id,
    employee_name,
    position_id,
    positon_name,
    starton AS start_on,
    TO_CHAR(eventon, 'DD.MM.YYYY') AS event_on,
    week_day,
    first_entertime AS enter_at,
    last_exittime AS exit_at,
    first_enterschedule AS enter_at_schedule,
    last_exitschedule AS exit_at_schedule,
    SUM(periodMinute) AS period_minute,
    SUM(periodMinuteSchedule) AS period_minute_schedule,
	total_time AS period_minute_total,
	enter_count,
	exit_count
FROM
    cte4
GROUP BY
    employee_id,
    employee_name,
    position_id,
    positon_name,
    starton,
    eventon,
	first_entertime,
	last_exittime,
	first_enterschedule,
	last_exitschedule,
	enter_count,
	exit_count,
	total_time,
    week_day;