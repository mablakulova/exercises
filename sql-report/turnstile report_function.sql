CREATE OR REPLACE FUNCTION hrm.get_employee_turnstile_report_by_id
(
    in_start_date timestamp without time zone,
	in_end_date timestamp without time zone,
	in_org_id integer,
	in_language_id integer,
	in_employee_id integer,
	in_start_work_time interval,
	in_end_work_time interval
)
RETURNS TABLE 
(
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