WITH cte
AS (
	SELECT DISTINCT employeemanage.employee_id employee_id,
		employee.full_name employee_fullname,
		position.id position_id,
		COALESCE(positiontranslate.translate_text::CHARACTER VARYING, position.short_name) positon_name,
		TO_CHAR(employeemanage.start_on, 'DD.MM.YYYY') starton,
		TO_CHAR((employeelog.event_At - INTERVAL '5 hours')::DATE, 'DD.MM.YYYY') eventon,
		employeelog.employee_turnstile_log_type_id action_type,
		TRIM(BOTH ' ' FROM TO_CHAR((employeelog.event_At - INTERVAL '5 hours')::DATE, 'Day')) AS week_day,
		DATE_TRUNC('minute', employeelog.event_at) event_at,
		scheduledayhour.begin_at::interval begin_schedule_time,
		scheduledayhour.end_at::interval end_schedule_time,
		row_number() OVER (ORDER BY employeelog.event_at) AS rn FROM hrm.sys_employee_turnstile_log employeelog LEFT JOIN hrm.sys_employee_manage employeemanage
		ON employeelog.employee_id = employeemanage.employee_id LEFT JOIN PUBLIC.hl_employee employee
		ON employeemanage.employee_id = employee.id LEFT JOIN PUBLIC.hl_position position
		ON employeemanage.position_id = position.id LEFT JOIN hrm.info_work_schedule schedule
		ON schedule.id = employeemanage.work_schedule_id LEFT JOIN hrm.info_work_schedule_day_hour scheduledayhour
		ON scheduledayhour.owner_id = schedule.id LEFT JOIN PUBLIC.hl_position_translate positiontranslate
		ON positiontranslate.owner_id = position.id WHERE employee.id = 232
		AND (employeelog.event_At - INTERVAL '5 hours')::DATE = '2024-03-27'
		AND scheduledayhour.day_number = EXTRACT(ISODOW FROM (employeelog.event_At - INTERVAL '5 hours')::DATE::TIMESTAMP)
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
				THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS exittime FROM cte a LEFT JOIN (
		SELECT employee_id,
			event_at,
			rn FROM cte WHERE action_type = 1
		) AS entertime
		ON a.employee_id = entertime.employee_id
			AND a.rn = entertime.rn LEFT JOIN (
		SELECT employee_id,
			event_at,
			rn FROM cte WHERE action_type = 2
		) AS exittime
		ON a.employee_id = exittime.employee_id
			AND a.rn = exittime.rn - 1 ORDER BY CASE 
			WHEN entertime.event_at IS NULL
				THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
			ELSE entertime.event_at
			END
	),
cte3
AS (
	SELECT cte2.*,
		CASE 
			WHEN (DATE_PART('day', breaklogic.officeouttime - breaklogic.officeintime) * 24 + DATE_PART('hour', breaklogic.officeouttime - breaklogic.officeintime) * 60 + DATE_PART('minute', breaklogic.officeouttime - breaklogic.officeintime)) * - 1 <= 60
				THEN (DATE_PART('day', cte2.exittime - cte2.entertime) * 24 + DATE_PART('hour', cte2.exittime - cte2.entertime) * 60 + DATE_PART('minute', cte2.exittime - cte2.entertime)) - (60 - (DATE_PART('day', breaklogic.officeouttime - breaklogic.officeintime) * 24 + DATE_PART('hour', breaklogic.officeouttime - breaklogic.officeintime) * 60 + DATE_PART('minute', breaklogic.officeouttime - breaklogic.officeintime)) * - 1)
			ELSE (DATE_PART('day', cte2.exittime - cte2.entertime) * 24 + DATE_PART('hour', cte2.exittime - cte2.entertime) * 60 + DATE_PART('minute', cte2.exittime - cte2.entertime))
			END periodMinute,
		(DATE_PART('hour', cte2.exitschedule - cte2.enterschedule) * 60 + DATE_PART('minute', cte2.exitschedule - cte2.enterschedule)) periodMinuteSchedule FROM cte2 LEFT JOIN (
		SELECT aa.employee_Id,
			aa.eventon,
			aa.entertime,
			aa.exittime,
			aa.officeouttime,
			aa.officeintime FROM (
			SELECT employee_Id,
				eventon,
				entertime,
				exittime,
				CASE 
					WHEN DATE_PART('hour', exittime) * 60 + DATE_PART('minute', exittime) <= 870
						THEN exittime
					ELSE NULL
					END AS officeouttime,
				lead(CASE 
						WHEN DATE_PART('hour', entertime) * 60 + DATE_PART('minute', entertime) >= 720
							THEN entertime
						ELSE NULL
						END) OVER (
					PARTITION BY employee_id,
					eventon ORDER BY entertime
					) AS officeintime FROM cte2 WHERE eventon::DATE >= '2024-03-27'::DATE
				AND eventon::DATE <= '2024-03-27'::DATE GROUP BY employee_Id,
				entertime,
				eventon,
				exittime
			) aa WHERE officeouttime IS NOT NULL
		) breaklogic
		ON breaklogic.employee_Id = cte2.employee_id
			AND --breaklogic.eventOn = cte2.eventOn  and 
			breaklogic.exittime = cte2.exittime
			AND breaklogic.entertime = cte2.entertime WHERE (
			cte2.entertime IS NOT NULL
			AND cte2.exittime IS NOT NULL
			)
		AND (cte2.entertime <> cte2.exittime)
	) SELECT * FROM cte3