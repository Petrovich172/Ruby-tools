require 'active_record'
require 'activerecord-postgis-adapter'

ActiveRecord::Base.establish_connection(
    {
     :adapter => 'postgis',
     :postgis_extension => true,
     :host => '10.176.70.81',
     :database => 'kronosdb',
     :port => '5432',
     :username => 'GorokhovIA',
     :password => 'KrjA6OQoUn'}
     ) #.prepend Reconnect


p ActiveRecord::Base.exeq_query("
	select vehicle_id, driver_id from parkon.trip_sheets where trip_sheet_date = now()::date and disabled is not true and driver_id is not null and vehicle_id is not null order by vehicle_id
	")