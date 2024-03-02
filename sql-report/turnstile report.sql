WITH cte
AS (
	SELECT DISTINCT empmanage.employee_id Employee_ID,
		emp.full_name EmployeeFullName,
		pos.id PositionID,
		COALESCE(postran.translate_text::CHARACTER VARYING, pos.short_name) PositionName,
		TO_CHAR(empmanage.start_on, 'DD.MM.YYYY') StartOn,
		TO_CHAR(emplog.event_on, 'DD.MM.YYYY') EventOn,
		emplog.employee_turnstile_log_type_id ActionType,
		TRIM(BOTH ' ' FROM TO_CHAR(emplog.event_on, 'Day')) AS WeekDay,
		DATE_TRUNC('minute', emplog.event_at) event_at,
		scheduleday.begin_at::interval BeginScheduleTime,
		scheduleday.end_at::interval EndScheduleTime,
		emplog.date_of_created,
		row_number() OVER (ORDER BY emplog.event_at) AS rn
	FROM hrm.sys_employee_turnstile_log emplog
	LEFT JOIN hrm.sys_employee_manage empmanage
		ON emplog.employee_id = empmanage.employee_id
	LEFT JOIN PUBLIC.hl_employee emp
		ON empmanage.employee_id = emp.id
	LEFT JOIN PUBLIC.hl_position pos
		ON empmanage.position_id = pos.id
	LEFT JOIN hrm.info_work_schedule schedule
		ON schedule.id = empmanage.work_schedule_id
	LEFT JOIN hrm.info_work_schedule_day_hour scheduleday
		ON scheduleday.owner_id = schedule.id
	LEFT JOIN PUBLIC.hl_position_translate postran
		ON postran.owner_id = pos.id
	WHERE emp.id = 20 AND emplog.event_on = '2024-02-16' AND scheduleday.day_number = EXTRACT(ISODOW FROM emplog.event_on::TIMESTAMP)
	),
cte2
AS (
	SELECT a.Employee_ID,
		a.EmployeeFullName,
		a.PositionID,
		a.PositionName,
		a.StartOn,
		a.EventOn,
		a.WeekDay,
		CASE 
			WHEN CASE 
					WHEN a.BeginScheduleTime >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.BeginScheduleTime
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END >= a.EndScheduleTime
				THEN a.EndScheduleTime
			ELSE CASE 
					WHEN a.BeginScheduleTime >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.BeginScheduleTime
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END
			END AS enterschedule,
		CASE 
			WHEN a.EndScheduleTime <= TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval
				THEN a.EndScheduleTime
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
		WHERE actiontype = 1
		) AS entertime
		ON a.employee_id = entertime.employee_id AND a.rn = entertime.rn
	LEFT JOIN (
		SELECT employee_id,
			event_at,
			rn
		FROM cte
		WHERE actiontype = 2
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