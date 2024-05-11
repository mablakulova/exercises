--DROP FUNCTION IF EXISTS hrm.get_employee_turnstile_report(timestamp without time zone, timestamp without time zone, integer, integer, interval, interval, character varying);

CREATE OR REPLACE FUNCTION hrm.get_employee_turnstile_report_test1(
	in_start_date timestamp without time zone,
	in_end_date timestamp without time zone,
	in_org_id integer,
	in_language_id integer,
	in_start_work_time interval,
	in_end_work_time interval,
	in_employee_fullname character varying
)
RETURNS TABLE (
    employee_id integer, 
    employee_name character varying, 
    position_id integer, 
    position_name character varying, 
    start_on text, 
    event_on text, 
    week_day text, 
    enter_at text, 
    exit_at text, 
    enter_at_schedule text, 
    exit_at_schedule text, 
    period_minute integer, 
    period_minute_schedule integer, 
    period_minute_total integer, 
    enter_count bigint,
    exit_count bigint
) 
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1000
AS $BODY$

BEGIN
   RETURN QUERY 
   WITH cte
   AS (
	SELECT 
        DISTINCT employeemanage.employee_id,
		person.full_name AS employee_name,
		position.id position_id,
		COALESCE(positiontranslate.translate_text::CHARACTER VARYING, position.short_name) AS positon_name,
		TO_CHAR(employeemanage.start_on, 'DD.MM.YYYY') AS starton,
		(employeelog.event_at - INTERVAL '7 hours')::DATE AS eventon,
		employeelog.employee_turnstile_log_type_id AS action_type,
		TRIM(BOTH ' ' FROM TO_CHAR((employeelog.event_At - INTERVAL '7 hours')::DATE, 'Day')) AS week_day,
		DATE_TRUNC('minute', employeelog.event_at) AS event_at,
	    TO_CHAR(DATE_TRUNC('minute', employeelog.event_at), 'HH24:MI')::INTERVAL AS event_at_interval,
	    scheduledayhour.begin_at::INTERVAL AS schedule_day_begin_hour,
	    scheduledayhour.end_at::INTERVAL AS schedule_day_end_hour,
		COALESCE(in_start_work_time, scheduledayhour.begin_at::interval, '09:00'::interval) AS begin_schedule_time,
		COALESCE(in_end_work_time, scheduledayhour.end_at::interval, '18:00'::interval) AS end_schedule_time,
	    ROW_NUMBER() OVER (PARTITION BY employeelog.employee_id, (employeelog.event_At - INTERVAL '7 hours')::DATE ORDER BY employeelog.event_at) AS rn,
	    COUNT(*) OVER () AS row_count
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
        scheduledayhour.day_number = EXTRACT(ISODOW FROM (employeelog.event_at - INTERVAL '7 hours')::DATE::TIMESTAMP)
    LEFT JOIN 
	    public.info_position_translate positiontranslate ON (in_language_id IS NOT NULL AND positiontranslate.language_id = in_language_id 
		    AND positiontranslate.column_name = 'short_name' AND positiontranslate.owner_id = position.id)
    WHERE 
        (in_employee_fullname IS NULL OR person.full_name ILIKE ('%' || in_employee_fullname || '%')) AND 
        employeemanage.start_on < in_end_date::DATE AND
	    (employeemanage.end_on IS NULL OR employeemanage.end_on > in_end_date::DATE) AND
        employeemanage.is_deleted = false AND
        employee.organization_id = in_org_id AND
        (employeelog.event_at - INTERVAL '7 hours')::DATE >= in_start_date::DATE AND 
        (employeelog.event_at - INTERVAL '7 hours')::DATE <= in_end_date::DATE	
	),
   cte2
    AS (
	  SELECT 
        a.employee_id,
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
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END >= a.end_schedule_time
				THEN a.end_schedule_time
			ELSE CASE 
					WHEN a.begin_schedule_time >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END
			END AS enterschedule,
		CASE 
			WHEN a.end_schedule_time <= TO_CHAR(CASE 
						                           WHEN exittime.event_at IS NULL
						                              THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						                        ELSE exittime.event_at
						                        END, 'HH24:MI')::interval
				OR TO_CHAR(CASE 
						      WHEN exittime.event_at IS NULL
							     THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						   ELSE exittime.event_at
						   END, 'HH24:MI')::interval <= '07:00'::interval
				THEN a.end_schedule_time
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN a.schedule_day_end_hour
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN '18:00'::INTERVAL
               ELSE event_at_interval
               END
		    WHEN (a.row_count = 1 AND a.eventon = CURRENT_DATE AND a.action_type = 1) 
		       THEN TO_CHAR(NOW(), 'HH24:MI')::INTERVAL
			ELSE TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval
	        END AS exitschedule,
		CASE 
			WHEN entertime.event_at IS NULL
				THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
			ELSE entertime.event_at
			END AS entertime,
		CASE 
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN (a.event_at::DATE || ' ' || a.schedule_day_end_hour)::TIMESTAMP
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN (a.event_at::DATE || ' ' || '18:00'::INTERVAL)::TIMESTAMP
               ELSE a.event_at
               END
		    WHEN (a.row_count = 1 AND a.eventon = CURRENT_DATE AND a.action_type = 1) 
		       THEN NOW()
		    WHEN exittime.event_at IS NULL
               THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS exittime,
		CASE 
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN (a.event_at::DATE || ' ' || a.schedule_day_end_hour)::TIMESTAMP
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN (a.event_at::DATE || ' ' || '18:00'::INTERVAL)::TIMESTAMP
               ELSE a.event_at
               END
		    WHEN exittime.event_at IS NULL
               THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS d_exittime
		FROM cte a 
		LEFT JOIN (
			SELECT cte.employee_id,
			       event_at,
			       eventon,
			       rn 
		    FROM cte 
		    WHERE action_type = 1
		    ) AS entertime
		    ON a.employee_id = entertime.employee_id AND a.rn = entertime.rn AND a.eventon = entertime.eventon
	    LEFT JOIN (
			SELECT cte.employee_id,
			       event_at,
			       eventon,
			       rn 
			FROM cte 
			WHERE action_type = 2
		    ) AS exittime
		    ON a.employee_id = exittime.employee_id AND a.rn = exittime.rn - 1 AND a.eventon = exittime.eventon
		ORDER BY CASE 
			WHEN entertime.event_at IS NULL
		        THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
			ELSE entertime.event_at
			END
	),
    cte3
      AS (
        SELECT 
	        cte2.employee_id,
		    cte2.eventon,
            MIN(entertime) AS first_entertime,
            MAX(exittime) AS last_exittime,
		    MAX(d_exittime) AS last_d_exittime,
            MIN(enterschedule) AS first_enterschedule,
            MAX(exitschedule) AS last_exitschedule,
            COUNT(entertime) AS enter_count,
	        COUNT(d_exittime) AS exit_count
        FROM 
            cte2
        WHERE 
            (entertime IS NOT NULL AND exittime IS NOT NULL) AND 
	        (entertime <> exittime)
	    GROUP BY 
            cte2.employee_id, cte2.eventon
       ),
    cte4
      AS (
        SELECT 
            cte2.*,
	        cte3.first_entertime,
	        cte3.last_exittime,
		    cte3.last_d_exittime,
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
           cte3 ON cte2.employee_id = cte3.employee_id AND cte2.eventon = cte3.eventon
        WHERE 
           (cte2.entertime IS NOT NULL AND cte2.exittime IS NOT NULL) AND 
           (cte2.entertime <> cte2.exittime)
      )
    SELECT
        cte4.employee_id,
        cte4.employee_name,
        cte4.position_id,
        cte4.positon_name,
        cte4.starton AS start_on,
        TO_CHAR(cte4.eventon, 'DD.MM.YYYY') AS event_on,
        cte4.week_day,
        TO_CHAR(cte4.first_entertime, 'HH24:MI:SS') AS enter_at,
        CASE
           WHEN (cte4.last_exittime = cte4.last_d_exittime AND cte4.last_d_exittime IS NOT NULL)
              THEN TO_CHAR(cte4.last_exittime, 'HH24:MI:SS') 
		   ELSE NULL
        END exit_at,
        TO_CHAR(cte4.first_enterschedule, 'HH24:MI:SS') AS enter_at_schedule,
        TO_CHAR(cte4.last_exitschedule, 'HH24:MI:SS') AS exit_at_schedule,
        CAST(SUM(cte4.periodMinute) AS INTEGER) AS period_minute,
        CAST(SUM(cte4.periodMinuteSchedule) AS INTEGER) AS period_minute_schedule,
	    CAST(cte4.total_time AS INTEGER) AS period_minute_total,
        cte4.enter_count,
	    cte4.exit_count
    FROM
        cte4
    GROUP BY
        cte4.employee_id,
        cte4.employee_name,
        cte4.position_id,
        cte4.positon_name,
        cte4.starton,
        cte4.eventon,
	    cte4.first_entertime,
	    cte4.last_exittime,
		cte4.last_d_exittime,
	    cte4.first_enterschedule,
	    cte4.last_exitschedule,
        cte4.enter_count,
	    cte4.exit_count,
	    cte4.total_time,
        cte4.week_day
    ORDER BY
        cte4.eventon;

END;
$BODY$;

--DROP FUNCTION IF EXISTS hrm.get_employee_turnstile_report_by_id(timestamp without time zone, integer, integer, integer);

CREATE OR REPLACE FUNCTION hrm.get_employee_turnstile_report_by_id_test1(
	in_start_date timestamp without time zone,
	in_end_date timestamp without time zone,
	in_org_id integer,
	in_language_id integer,
	in_employee_id integer,
	in_start_work_time interval,
	in_end_work_time interval
)
RETURNS TABLE (
    employee_id integer, 
    event_on date, 
    week_day text, 
    enter_at text, 
    exit_at text, 
    period_minute integer, 
    enter_at_schedule text, 
    exit_at_schedule text, 
    period_minute_schedule integer
) 
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1000
AS $BODY$

BEGIN 
   RETURN QUERY
   WITH cte
   AS (
	SELECT 
        DISTINCT employeemanage.employee_id,
		(employeelog.event_at - INTERVAL '7 hours')::DATE AS eventon,
		employeelog.employee_turnstile_log_type_id AS action_type,
		TRIM(BOTH ' ' FROM TO_CHAR((employeelog.event_At - INTERVAL '7 hours')::DATE, 'Day')) AS week_day,
		DATE_TRUNC('minute', employeelog.event_at) AS event_at,
	    TO_CHAR(DATE_TRUNC('minute', employeelog.event_at), 'HH24:MI')::INTERVAL AS event_at_interval,
	    scheduledayhour.begin_at::INTERVAL AS schedule_day_begin_hour,
	    scheduledayhour.end_at::INTERVAL AS schedule_day_end_hour,
		COALESCE(in_start_work_time, scheduledayhour.begin_at::interval, '09:00'::interval) AS begin_schedule_time,
		COALESCE(in_end_work_time, scheduledayhour.end_at::interval, '18:00'::interval) AS end_schedule_time,
	    ROW_NUMBER() OVER (PARTITION BY employeelog.employee_id, (employeelog.event_At - INTERVAL '7 hours')::DATE ORDER BY employeelog.event_at) AS rn,
	    COUNT(*) OVER () AS row_count
    FROM 
        hrm.sys_employee_turnstile_log employeelog 
    LEFT JOIN 
        hrm.sys_employee_manage employeemanage ON employeelog.employee_id = employeemanage.employee_id 
    LEFT JOIN 
        hrm.hl_employee employee ON employeemanage.employee_id = employee.id 
    LEFT JOIN 
        public.hl_person person ON employee.person_id = person.id
    LEFT JOIN 
        hrm.info_work_schedule schedule ON schedule.id = employeemanage.work_schedule_id 
    LEFT JOIN 
        hrm.info_work_schedule_day_hour scheduledayhour ON scheduledayhour.owner_id = schedule.id AND
        scheduledayhour.day_number = EXTRACT(ISODOW FROM (employeelog.event_at - INTERVAL '7 hours')::DATE::TIMESTAMP)
    WHERE
	    employeelog.employee_id = in_employee_id AND
        employeemanage.start_on < in_end_date::DATE AND
	    (employeemanage.end_on IS NULL OR employeemanage.end_on > in_end_date::DATE) AND
        employeemanage.is_deleted = false AND
        employee.organization_id = in_org_id AND
        (employeelog.event_at - INTERVAL '7 hours')::DATE >= in_start_date::DATE AND 
        (employeelog.event_at - INTERVAL '7 hours')::DATE <= in_end_date::DATE	
	),
   cte2
    AS (
	  SELECT 
        a.employee_id,
		a.eventon,
		a.week_day,
		CASE 
			WHEN CASE 
					WHEN a.begin_schedule_time >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END >= a.end_schedule_time
				THEN a.end_schedule_time
			ELSE CASE 
					WHEN a.begin_schedule_time >= TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
									THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
						THEN a.begin_schedule_time
					ELSE TO_CHAR(CASE 
								WHEN entertime.event_at IS NULL
								    THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
								ELSE entertime.event_at
								END, 'HH24:MI')::interval
					END
			END AS enterschedule,
		CASE 
			WHEN a.end_schedule_time <= TO_CHAR(CASE 
						                           WHEN exittime.event_at IS NULL
						                              THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						                        ELSE exittime.event_at
						                        END, 'HH24:MI')::interval
				OR TO_CHAR(CASE 
						      WHEN exittime.event_at IS NULL
							     THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						   ELSE exittime.event_at
						   END, 'HH24:MI')::interval <= '07:00'::interval
				THEN a.end_schedule_time
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN a.schedule_day_end_hour
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN '18:00'::INTERVAL
               ELSE event_at_interval
               END
		    WHEN (a.row_count = 1 AND a.eventon = CURRENT_DATE AND a.action_type = 1) 
		       THEN TO_CHAR(NOW(), 'HH24:MI')::INTERVAL
			ELSE TO_CHAR(CASE 
						WHEN exittime.event_at IS NULL
							THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
						ELSE exittime.event_at
						END, 'HH24:MI')::interval
	        END AS exitschedule,
		CASE 
			WHEN entertime.event_at IS NULL
				THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
			ELSE entertime.event_at
			END AS entertime,
		CASE 
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN (a.event_at::DATE || ' ' || a.schedule_day_end_hour)::TIMESTAMP
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN (a.event_at::DATE || ' ' || '18:00'::INTERVAL)::TIMESTAMP
               ELSE a.event_at
               END
		    WHEN (a.row_count = 1 AND a.eventon = CURRENT_DATE AND a.action_type = 1) 
		       THEN NOW()
		    WHEN exittime.event_at IS NULL
               THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS exittime,
		CASE 
		    WHEN (a.row_count = 1 AND a.eventon < CURRENT_DATE AND a.action_type = 1) THEN
		       CASE
		           WHEN event_at_interval < a.schedule_day_end_hour 
		              THEN (a.event_at::DATE || ' ' || a.schedule_day_end_hour)::TIMESTAMP
                   WHEN event_at_interval > a.schedule_day_end_hour AND event_at_interval < '18:00'::INTERVAL 
		              THEN (a.event_at::DATE || ' ' || '18:00'::INTERVAL)::TIMESTAMP
               ELSE a.event_at
               END
		    WHEN exittime.event_at IS NULL
               THEN LEAD(entertime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY entertime.rn)
			ELSE exittime.event_at
			END AS d_exittime
		FROM cte a 
		LEFT JOIN (
			SELECT cte.employee_id,
			       event_at,
			       eventon,
			       rn 
		    FROM cte 
		    WHERE action_type = 1
		    ) AS entertime
		    ON a.employee_id = entertime.employee_id AND a.rn = entertime.rn AND a.eventon = entertime.eventon
	    LEFT JOIN (
			SELECT cte.employee_id,
			       event_at,
			       eventon,
			       rn 
			FROM cte 
			WHERE action_type = 2
		    ) AS exittime
		    ON a.employee_id = exittime.employee_id AND a.rn = exittime.rn - 1 AND a.eventon = exittime.eventon
		ORDER BY CASE 
			WHEN entertime.event_at IS NULL
		        THEN LAG(exittime.event_at) OVER (PARTITION BY a.employee_id, a.eventon ORDER BY exittime.rn)
			ELSE entertime.event_at
			END
	),
    cte3
      AS (
        SELECT 
            cte2.employee_id,
		    cte2.eventon,
		    cte2.week_day,
		    TO_CHAR(cte2.entertime, 'HH24:MI:SS') AS enter_at,
		    TO_CHAR(cte2.d_exittime, 'HH24:MI:SS') AS exit_at,
            CAST((DATE_PART('day', cte2.exittime - cte2.entertime) * 24 + DATE_PART('hour', cte2.exittime - cte2.entertime) * 60 + 
             DATE_PART('minute', cte2.exittime - cte2.entertime)) AS INTEGER) period_minute,
		    TO_CHAR(cte2.enterschedule, 'HH24:MI:SS') AS enter_at_schedule,
            TO_CHAR(cte2.exitschedule, 'HH24:MI:SS') AS exit_at_schedule,
            CAST((DATE_PART('day', cte2.exitschedule - cte2.enterschedule) * 24 + DATE_PART('hour', cte2.exitschedule - cte2.enterschedule) * 60 + 
             DATE_PART('minute', cte2.exitschedule - cte2.enterschedule)) AS INTEGER)  period_minute_schedule
        FROM
           cte2
        WHERE 
           (cte2.entertime IS NOT NULL AND cte2.exittime IS NOT NULL) AND 
           (cte2.entertime <> cte2.exittime)
      )
    SELECT * FROM cte3;

END;
$BODY$;