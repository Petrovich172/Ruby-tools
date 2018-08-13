# module Reconnect
#   def execute(*args)
#     # During `reconnect!`, `Mysql2Adapter` first disconnect and set the
#     # @connection to nil, and then tries to connect. When connect fails,
#     # @connection will be left as nil value which will cause issues later.
#     connect if @connection.nil?

#     begin
#       super(*args)
#     rescue ActiveRecord::StatementInvalid => e
#       if e.message =~ /server has gone away/i
#         in_transaction = transaction_manager.current_transaction.open?
#         try_reconnect
#         in_transaction ? raise : retry
#       else
#         raise
#       end
#     end
#   end

#   private
#   def try_reconnect
#     sleep_times = [0.1, 0.5, 1, 2, 4, 8]

#     begin
#       reconnect!
#     rescue Error => e
#       sleep_time = sleep_times.shift
#       if sleep_time && e.message =~ /can't connect/i
#         warn "Server timed out, retrying in #{sleep_time} sec."
#         sleep sleep_time
#         retry
#       else
#         raise
#       end
#     end
#   end
# end

require 'active_record'
require 'activerecord-postgis-adapter'

def check_4_nil(argument)
	if argument.rows[0].class == Array and argument.rows[0][0] == nil # Костыль на случай, когда запрос возвращает нуль внутри массива типа: argument.rows[0] = [nil]
		argument = "null"
		# p "Empty inner array"
	elsif argument.rows == nil or argument.rows[0] == nil
		argument = "null"
		# p "Empty query itself"
	elsif argument.rows[0].size > 1
		# p "Array size > 1"
		argument = argument.rows[0]
	elsif argument.rows[0].size <= 1
		# p "Array size <= 1"
		argument = argument.rows[0][0]
	else argument = "null"
		# p "Else case"
	end
	# p argument
	return argument
end

def check_4_nil_2(argument)
	# p argument
	if  argument == nil
		# p "nil condition"
		argument = "null"
	elsif "#{argument}" != "null"
		argument = (("'"+("#{argument}")+"'") )
	end
	return argument
end

begin # begin for rescue

loop do # Whole cycle start

today = Time.now.strftime('%Y-%m-%d')
puts "New day has come! It's #{today}"

(while Time.now.hour < 2 do (sleep 1800 and p "waiting for the orders being downloaded (around 2am)")
end)


ActiveRecord::Base.establish_connection(
    {
     :adapter => 'postgis',
     :postgis_extension => true,
     :host => '10.176.70.84',
     :database => 'kronosdb',
     :port => '5432',
     :username => 'GorokhovIA',
     :password => 'KrjA6OQoUn'}
     ) #.prepend Reconnect

orders = ActiveRecord::Base.connection.exec_query(
	"select vehicle_id, driver_id from parkon.trip_sheets where trip_sheet_date = now()::date and disabled is not true and driver_id is not null and vehicle_id is not null order by vehicle_id
	")

(while orders.rows.size < 1
	puts "No orders! Check out orders loading script or main DB" ;p
	sleep 7200
	orders = ActiveRecord::Base.connection.exec_query(
	"select vehicle_id, driver_id from parkon.trip_sheets where trip_sheet_date = now()::date and disabled is not true and driver_id is not null and vehicle_id is not null order by vehicle_id
	")
	# redo
end)

loop do #Every day cycle start
begin_time = Time.now

cleaning_dublicates = ActiveRecord::Base.connection.exec_query(
	"delete from park_analytics.reports_summary where status_time::date = now()::date and guid not in
        (
		select DISTINCT on (status_time, driver_name, gos_num) guid from park_analytics.reports_summary where status_time || driver_name || gos_num in 
            (
            select max(status_time) || driver_name || gos_num from park_analytics.reports_summary where status_time::date = now()::date group by driver_name, gos_num
            )
		)
	") and p "cleaning_dublicates"
cleaning_routes = ActiveRecord::Base.connection.exec_query(
	"delete from park_calculated.logs_route_left where guid not in (select distinct on (driver_id, vehicle_id, from_ts) guid from park_calculated.logs_route_left order by from_ts desc)
	") and p "cleaning_routes"
cleaning_stops = ActiveRecord::Base.connection.exec_query(
	"delete from park_calculated.logs_stops where guid not in (select distinct on (driver_id, vehicle_id, from_ts) guid from park_calculated.logs_stops order by from_ts desc)
	") and p "cleaning_stops"
cleaning_speeds = ActiveRecord::Base.connection.exec_query(
	"delete from park_calculated.logs_speeds where guid not in (select distinct on (driver_id, vehicle_id, timestamp) guid from park_calculated.logs_speeds order by timestamp desc)
	") and p "cleaning_speeds"
cleaning_statuses = ActiveRecord::Base.connection.exec_query(
	"delete from park_calculated.logs_status where guid not in (select distinct on (driver_id, vehicle_id, status_time) guid from park_calculated.logs_status order by status_time desc)
	") and p "cleaning_statuses"

sleep 1
point_line = 0
print "Logging ..." ;p
orders.each{|order|
	driver_id = "#{order['driver_id']}"
	vehicle_id = "#{order['vehicle_id']}"

	driver_name = check_4_nil( ActiveRecord::Base.connection.exec_query("select last_name || ' ' || first_name || ' ' || middle_name from parkon.drivers where id = '#{driver_id}' ") )
	gos_num = check_4_nil( ActiveRecord::Base.connection.exec_query("select reg_number from parkon.vehicles where id = '#{vehicle_id}' ") )
	status = check_4_nil( ActiveRecord::Base.connection.exec_query("select _status from parkon._driver_id_status('#{driver_id}')") )
	route_lefts = check_4_nil( ActiveRecord::Base.connection.exec_query("select count(distinct from_ts) from park_calculated.logs_route_left where driver_id = '#{driver_id}' and from_ts::date = now()::date") )
	route_absence_time = check_4_nil( ActiveRecord::Base.connection.exec_query("SELECT sum(route_abcence_interval) from (select distinct on (from_ts) route_abcence_interval FROM park_calculated.logs_route_left where driver_id = '#{driver_id}' and from_ts::date = now()::date ) as for_sume") )
	stops = check_4_nil( ActiveRecord::Base.connection.exec_query("select count(distinct from_ts) from park_calculated.logs_stops where driver_id = '#{driver_id}' and from_ts::date = now()::date") )
	_stop_time = check_4_nil( ActiveRecord::Base.connection.exec_query("SELECT sum(stop_time) from (select distinct on (from_ts) stop_time FROM park_calculated.logs_stops where driver_id = '#{driver_id}' and from_ts::date = now()::date ) as for_sume") )
	speeds = check_4_nil( ActiveRecord::Base.connection.exec_query("select count(guid) from park_calculated.logs_speeds where vehicle_id = '#{vehicle_id}' and timestamp::date = now()::date ") )
##old	# fix_zones = check_4_nil( ActiveRecord::Base.connection.exec_query("select sum(plan) || '/' || sum(fact) from park_analytics.fix_zones_logging_full('#{driver_id}', '#{vehicle_id}', now()::date )") )
	fix_zones = check_4_nil( ActiveRecord::Base.connection.exec_query("select sum(plan_zones) || '/' || sum(fact_zones) from park_calculated.logs_fix_zones where vehicle_id = '#{vehicle_id}' and driver_id = '#{driver_id}' and log_time::date = now()::date") )
	last_event = check_4_nil( ActiveRecord::Base.connection.exec_query("select name, from_ts, geom from parkon_stat.working_logs as wl join parkon.working_log_types as lt on wl.log_type = lt.id where driver_id = '#{driver_id}' and vehicle_id = '#{vehicle_id}' and log_type in ('ONR0', 'MOU0', 'SPD1') and (from_ts between now()::date::timestamptz and now()::timestamptz) order by from_ts desc limit 1") )
	(geom_point = "null" and status_time = "null" and last_event = "null") if last_event.nil? or last_event == "null"
	status_time = last_event[1].to_datetime.localtime if ! (last_event.nil? or last_event == "null")
	geom_point = last_event[2] if ! (last_event.nil? or last_event == "null")
	last_event = last_event[0] if ! (last_event.nil? or last_event == "null")
	shift_start = check_4_nil( ActiveRecord::Base.connection.exec_query("select work_start_time from parkon.trip_sheets where driver_id = '#{driver_id}' and vehicle_id = '#{vehicle_id}' and trip_sheet_date = now()::date and disabled is false") )
	shift_start = shift_start.to_datetime.localtime if ! (shift_start.nil? or shift_start == "null")
	shift_end = check_4_nil( ActiveRecord::Base.connection.exec_query("select work_end_time from parkon.trip_sheets where driver_id = '#{driver_id}' and vehicle_id = '#{vehicle_id}' and trip_sheet_date = now()::date and disabled is false") )
	shift_end = shift_end.to_datetime.localtime if ! (shift_end.nil? or shift_end == "null")
	division = check_4_nil( ActiveRecord::Base.connection.exec_query("select name from parkon.divisions where id in (select division_id from parkon.vehicles where id = '#{vehicle_id}')") )
	route = check_4_nil( ActiveRecord::Base.connection.exec_query("SELECT string_agg(name::text, ', ') from parkon.routes where id in (select distinct(sr.route_id) FROM parkon.trip_sheet_tasks as st join parkon.trip_sheet_routes as sr on st.id = sr.trip_sheet_task_id where trip_sheet_id in (select id from parkon.trip_sheets where vehicle_id = '#{vehicle_id}' and driver_id = '#{driver_id}' and trip_sheet_date = now()::date and disabled is false) ) ") )
	_trip_sheet_no = check_4_nil( ActiveRecord::Base.connection.exec_query("select trip_sheet_no from parkon.trip_sheets where  driver_id = '#{driver_id}' and vehicle_id = '#{vehicle_id}' and trip_sheet_date = now()::date and disabled is false ") )


	# puts "route_left_logging for driver_id: #{driver_id}\t vehicle_id: #{vehicle_id}" ;p
	ActiveRecord::Base.connection.exec_query(
		"insert into park_calculated.logs_route_left (from_ts, next_time, route_abcence_interval, vehicle_id, driver_id, trip_sheet_id, geom, break_mark)
		select from_ts, next_time, route_abcence_interval, vehicle_id, driver_id, trip_sheet_id, geom, break_mark from park_analytics.route_left_logging('#{driver_id}', '#{vehicle_id}')
		"
		)
	# puts "speed_logging for vehicle_id #{vehicle_id}" ;p
	ActiveRecord::Base.connection.exec_query(
		"insert into park_calculated.logs_speeds (vehicle_id, driver_id, timestamp, permitted, actual, geom)
		select vehicle_id, driver_id, from_ts, regexp_replace((string_to_array(remark, ' '))[2], '[()]', '','g')::double precision, regexp_replace((string_to_array(remark, ' '))[1], '[()]', '','g')::double precision, geom 
		from parkon_stat.working_logs where vehicle_id = '#{vehicle_id}' and
		log_type = 'SPD1' AND remark IS NOT NULL and (from_ts between now()::date::timestamptz and now()::timestamptz) order by from_ts"
		)
	ActiveRecord::Base.connection.exec_query(
		"insert into park_calculated.logs_status (status_time, driver_name, driver_id, vehicle_id, _status)
		select from_ts, last_name, driver_id, vehicle_id, _status from parkon._driver_id_status('#{driver_id}')"
		)
	# puts "stop_logging for driver_id: #{driver_id}\t vehicle_id: #{vehicle_id}\n" ;p
	ActiveRecord::Base.connection.exec_query(
		"insert into park_calculated.logs_stops (from_ts, next_time, stop_time, vehicle_id, driver_id, trip_sheet_id, geom, break_mark)
		select from_ts, next_time, route_abcence_interval, vehicle_id, driver_id, trip_sheet_id, geom, break_mark from park_analytics.stop_logging('#{driver_id}', '#{vehicle_id}')
		"
		)
	ActiveRecord::Base.connection.exec_query(
		"delete from park_calculated.logs_fix_zones where driver_id = '#{driver_id}' and vehicle_id = '#{vehicle_id}' and log_time::date = now()::date
		"
		)
	ActiveRecord::Base.connection.exec_query("
		INSERT INTO park_calculated.logs_fix_zones (log_time, driver_id, vehicle_id, route_id, plan_zones, fact_zones, plan_links, fact_links, fact_zones_id, fact_links_id) 
		select log_time, driver_id, vehicle_id, route_id, plan_zones, fact_zones, plan_links, fact_links, fact_zones_id, fact_links_id from park_analytics.fix_zones_logging_full('#{driver_id}', '#{vehicle_id}')
		"
		)
	ActiveRecord::Base.connection.exec_query(
		"
		insert into park_analytics.reports_summary values (default, #{check_4_nil_2(driver_name)},     #{check_4_nil_2(gos_num)},     #{check_4_nil_2(status)},     #{check_4_nil_2(route_lefts)},     #{check_4_nil_2(route_absence_time)},     #{check_4_nil_2(stops)},     #{check_4_nil_2(_stop_time)},     #{check_4_nil_2(speeds)},     #{check_4_nil_2(fix_zones)},     #{check_4_nil_2(last_event)},     #{check_4_nil_2(status_time)},     #{check_4_nil_2(shift_start)},     #{check_4_nil_2(shift_end)},     #{check_4_nil_2(division)},     #{check_4_nil_2(route)},     #{check_4_nil_2(_trip_sheet_no)}, #{check_4_nil_2(driver_id)}, #{check_4_nil_2(vehicle_id)}, #{check_4_nil_2(geom_point)});
		"
		)
	point_line += 1
	(print "." ;p) if point_line < 85
	(point_line = 0 and puts "" ;p) if point_line >= 85
	}
puts "\n\n\n\nFinished at #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}. Time taken = #{(Time.now - begin_time).round(1)} seconds\s(#{( (Time.now - begin_time)/60 ).round(0) } min)" ;p
puts "\n\n\n**********___________Going to the new round!________**********\n\n\n" ;p
sleep 10
break if Time.now.strftime('%Y-%m-%d') != today
end #End of day cycle
end #End of whole cycle
rescue Exception => e
puts "#{e}"
end