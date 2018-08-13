# encoding: utf-8
require 'win32ole'
require 'net/http'
require 'date'

# Расчёт рейсов по двум точкам (список ТС)
############################################
############################################
############################################



date = "06.04.2016" # Начальная дата рассматриваемого диапазона
date_end = "06.04.2016" # Конечная дата рассматриваемого диапазона
time_begin = "07:00" # Время старта рассчёта работы ТС
time_end = "10:00" # Время старта рассчёта работы ТС
$loc1 = [55.741553, 37.655662] # Первая точка
$loc1_2 = [55.739076, 37.672989] # Вторая точка
$y = 250 # Радиус полигона рассматриваемых отметок
all_ts = ["100298", "100432", "100463", "100628"]
# all_ts = ["e84846"]



############################################
############################################
############################################



def distance(loc1, loc2)
    rad_per_deg = Math::PI/180  # PI / 180
    rkm = 6371                  # Earth radius in kilometers
    rm = rkm * 1000             # Radius in meters

    dlat_rad = (loc2[0]-loc1[0]) * rad_per_deg  # Delta, converted to rad
    dlon_rad = (loc2[1]-loc1[1]) * rad_per_deg

    lat1_rad, lon1_rad = loc1.map {|i| i * rad_per_deg }
    lat2_rad, lon2_rad = loc2.map {|i| i * rad_per_deg }

    a = Math.sin(dlat_rad/2)**2 + Math.cos(lat1_rad) * Math.cos(lat2_rad) * Math.sin(dlon_rad/2)**2
    c = 2 * Math::atan2(Math::sqrt(a), Math::sqrt(1-a))

    rm * c # Delta in meters
end

def get_speed_points(delta_t, loc1, loc2)
    s = distance(loc1, loc2)/1000
    return (s/(delta_t/3600)).round(1)
end

def get_speed_distance(delta_t, length) #Length in meters, delta_t in seconds
    s = length/1000
    t = delta_t.to_f/3600000
    return (s/t).round(1)
end

def get_as_body(link_service)
    sleep(1)    
    uri = URI('https://'+link_service)
    @http = Net::HTTP.new(uri.host, uri.port)
    @http.use_ssl = true # для https
    @http.verify_mode = OpenSSL::SSL::VERIFY_NONE # для https

    @http.start do |http|
      http.read_timeout = 300
      request = Net::HTTP::Get.new(uri.to_s)
      request.basic_auth "ageyev", "321"
      response = http.request(request)
      (@error = "Аутификация в еРНИС не прошла"; puts "Косяк с запросом response.code: #{response.code}"; return nil) unless response.is_a?(Net::HTTPSuccess)
      return response.body
    end
end

def get_navi_as_hash(link_service)
    result = {}
    body = get_as_body(link_service)
    if body.nil?
      puts = "Нет отметок навигации проблема с сервисом РНИС, link => #{link_service}, now => #{Time.now}, body => #{body}" 
      # puts @error if @view_reports      
    return result
    end

    status = body.scan(/\"status\":\"(.*?)\"/)[0][0]
    result["status"] = status
    body = body.scan(/\[{(.*)}\]/)[0]    
    
    type = body[0].scan(/\"type\":\"(.*?)\"/)[0][0]
    result[type] = []
    body[0].scan(/\"increment\":\[(.*?)\]/)[0][0].scan(/{(.*?)}/) {|string| string = string[0]
      temp_mas = {}
      string.split(',').each{|el| el = el.gsub('"','').split(':')
        temp_mas[el[0]] = el[1]
      }
      result[type] << temp_mas
    }
    return result
end

def get_navi_from_rnis(id_bnst, d_begin, d_end)
    (puts "[>] DB_write.get_navi_from_rnis..";p) if @view_reports
    # link = $http_links[:ernis_service][:com_navi].clone
    link = "10.176.70.21/vis/compasexpnavi/get?id=[???]&fromtime=[???]&deltatime=[???]".clone
    service_param = [id_bnst.to_s, d_begin.to_s.gsub('+00:00','.000+03:00'), (DateTime.parse(d_end).to_time - DateTime.parse(d_begin).to_time).round(0).to_s]
    service_param.each{|el| link.sub!("[???]",el)}    
    hash = get_navi_as_hash(link)
    return hash
end

def cut_navigation(hash)
  pre_point_time = hash["ExportAtt"][0]["time"].to_i/1000
  index_4_del = []
  hash["ExportAtt"].each_index{|index|
    point_time = hash["ExportAtt"][index]["time"].to_i/1000
    if (point_time - pre_point_time).abs < 10
      index_4_del << index
    else
      pre_point_time = hash["ExportAtt"][index]["time"].to_i/1000
    end
  }
  hash["ExportAtt"].delete_if.with_index{|el, index| index_4_del.include? index}
  puts "Число отфильтрованных навигационных отметок #{index_4_del.size}" ;p
  return hash
end

def check_for_movment(true_navi_mas, all_navi_mas, location)
    second_filter_points_mas = []
    true_navi_mas.each{|true_el|
      first_filter_points_mas = []
      first_filter_points_hash = {true_el => first_filter_points_mas}
      time_navi = DateTime.parse(Time.at(true_el["time"].to_i/1000).localtime($TimeZone).to_s)
      delta_time_min = DateTime.parse(Time.at((true_el["time"].to_i - 180000)/1000).localtime($TimeZone).to_s) #Диапазон времени мин 3 минуты
      delta_time_max = DateTime.parse(Time.at((true_el["time"].to_i + 180000)/1000).localtime($TimeZone).to_s) #Диапазон времени макс 3 минуты
      all_navi_mas["ExportAtt"].each{|el|
        time_diapozone = DateTime.parse(Time.at(el["time"].to_i/1000).localtime($TimeZone).to_s)
        if (time_diapozone > delta_time_min) and (time_diapozone < delta_time_max)
          first_filter_points_mas << el
        end
      }
      if # Делаем проверку на то, что последняя точка временного диапазона выходит за пределы обозначенного радиуса.
        distance(location, [(first_filter_points_hash.values.flatten[-1]["lat"]).to_f, (first_filter_points_hash.values.flatten[-1]["lon"]).to_f] ).round(0) > $y
        second_filter_points_mas << first_filter_points_hash.keys.flatten
      end
    }
    puts "Число точек после фильтрации на признак движения #{second_filter_points_mas.size}" ;p
    second_filter_points_mas.flatten.each{|el|
    }
    return second_filter_points_mas
end

def cut_navigation(hash)
    pre_point_time = hash["ExportAtt"][0]["time"].to_i/1000
    index_4_del = []
    hash["ExportAtt"].each_index{|index|
      point_time = hash["ExportAtt"][index]["time"].to_i/1000
      if (point_time - pre_point_time).abs < 10
        index_4_del << index
      else
        pre_point_time = hash["ExportAtt"][index]["time"].to_i/1000
      end
    }
    hash["ExportAtt"].delete_if.with_index{|el, index| index_4_del.include? index}
    puts "Число отфильтрованных навигационных отметок #{index_4_del.size}" ;p
    return hash
end

def time_in_reis_filter(array_of_true_points)
  if array_of_true_points.size != 0
    check_time = array_of_true_points[0]['time'].to_i/1000 if (array_of_true_points[0]['time'].to_i/1000 != nil)
    index_4_del = []
    array_of_true_points.each_index{|index|
      point_time = array_of_true_points[index]['time'].to_i/1000
      if (point_time - check_time).abs < 300 and (point_time - check_time).abs > 0 # Условие пяти минут. Отметка удаляется, если её отличие во времени от первой рассматриваемой меньше 5 минут. (Проверка на "простой")
        index_4_del << index
      else
        check_time = array_of_true_points[index]['time'].to_i/1000
      end
    }
    array_of_true_points.delete_if.with_index{|el, index| index_4_del.include? index}
    puts "Число отфильтрованных навигационных отметок #{index_4_del.size}" ;p    
    return array_of_true_points  
  end
end

def avg_of_times(array_of_times)
  avg = array_of_times.map{|el|
    el.to_i
  }.inject(:+)/array_of_times.size
  return Time.at(avg)  
end

def reis_begin_and_end(op1_visits_mas, op2_visits_mas)  
  index_4_del = []
  times = []
  op2_visits_mas.each{|visit_op2|
    # p DateTime.parse(Time.at(visit_op2.to_i/1000).localtime($TimeZone)).to_s
    temp = []
    op1_visits_mas.each{|visit_op1| # Для каждого времени посещения второй ОП ищем более раннее время посещения первой ОП. Формируем хэш-связку время посещения 1ОП => время посещения 2ОП
      temp << visit_op1 if visit_op1['time'] < visit_op2['time']
      }
      # temp.each{|el| p DateTime.parse(Time.at(el['time'].to_i/1000).localtime($TimeZone).to_s)}
      # p reis = {DateTime.parse(Time.at(temp[-1]['time'].to_i/1000).localtime($TimeZone).to_s) => DateTime.parse(Time.at(visit_op2["time"].to_i/1000).localtime($TimeZone).to_s)}
      reis = {temp[-1] => visit_op2} if temp[-1] != nil # Если для данной второй ОП нет более раннего времени посещения первой ОП — не засчитываем рейс
      times << reis
    }
    times.compact! # Удаляем пустые значения
  check = times[0]
    times.each_index{|index| # Чистим массив получившихся связок по принципу: берём первый элемент массива и сравниваем его со следующим. Удаляются элементы, у которых время посещения 1ОП совпадает, а время посещения 2ОП позже
      check_value = times[index].values[0]
      times[index]
      if (times[index].keys[0] != nil) and (times[index].keys[0]['time'] == check.keys[0]['time']) and (check.values[0]['time'] < times[index].values[0]['time'])
        index_4_del << index
      else
        check = times[index]
      end
    }
    times.delete_if.with_index{|el, index| index_4_del.include? index}
  return times
end

def kill_weekends (start_date_str, end_date_str)
 start_date_str = Date.parse(start_date_str) # your start date
 end_date_str = Date.parse(end_date_str) # your end date
 needed_days = [1,2,3,4,5] # Days of the week to leave in the result array. In 0-6. Sunday is day-of-week 0; Saturday is day-of-week 6.
 result = []
 (start_date_str..end_date_str).to_a.select{|el|
  result << el if needed_days.include?(el.wday)
    }
  return result
end

# Исключаем из диапазона дат выходные дни
no_weekends_dates_hash = {}
no_weekends_dates_array = kill_weekends(date, date_end).each{|el|  # Убираем из списка дат выходные
  no_weekends_dates_hash[el] = [(el.to_s + "\s" + time_begin), (el.to_s + "\s" + time_end)]
  }
all_reises_mas = []
bad_reises_mas = []
no_weekends_dates_hash.each{|date, times|
  date_begin = times[0]
  date_end = times[-1]
all_reises_count = 0
all_op_1_visits = []
all_op_2_visits = []

  #Проходимся по всем ТС
  all_ts.uniq!
    all_ts.each{|id_bnst|
      puts "\nРассматриваем Id_bnst: #{id_bnst}\tза дату: #{date}\n" ;p
    random_mas = []
    uniq_mas = []
    loc1 = $loc1
  begin # Бегин для рескью
    mas_navi = get_navi_from_rnis(id_bnst, DateTime.parse(date_begin).rfc3339, DateTime.parse(date_end).rfc3339)
    mas_navi = cut_navigation(mas_navi)
    puts "Массив навигации из РНИС получен. Расчитываем количество посещений указанной ОП ..." if (mas_navi.size != 0) ;p

    # Рассматриваем привязку массива навигации к первой остановке  
      mas_navi["ExportAtt"].each{|el|
        loc2_1 = [(el["lat"]).to_f, (el["lon"]).to_f]
         (uniq_mas << el) if (distance(loc1, loc2_1).round(0) < $y)
      }
      puts "Выбраны точки в радиусе #{$y}м от первой ОП" ;p
      check = check_for_movment(uniq_mas, mas_navi, loc1).flatten
      final_check_op_1 = time_in_reis_filter(check).flatten

    # Рассматриваем привязку массива навигации ко второй остановке
    uniq_mas = []
    loc1 = $loc1_2
      mas_navi["ExportAtt"].each{|el|
        loc2_2 = [(el["lat"]).to_f, (el["lon"]).to_f]
         (uniq_mas << el) if (distance(loc1, loc2_2).round(0) < $y)
      }
      puts "Выбраны точки в радиусе #{$y}м от второй ОП" ;p
      check = check_for_movment(uniq_mas, mas_navi, loc1).flatten
      final_check_op_2 = time_in_reis_filter(check).flatten

    # Фильтрация точек и формирование массива рейсов
      reis_hashes_array = reis_begin_and_end(final_check_op_1, final_check_op_2)
      reis_hashes_array.each{|reis|
        reis_points_mas = []
        distances_mas = []
        reis_end = DateTime.parse(Time.at((reis.values[0]['time'].to_i)/1000).localtime($TimeZone).to_s)
        reis_begin = DateTime.parse(Time.at((reis.keys[0]['time'].to_i)/1000).localtime($TimeZone).to_s)
        delta_t = (reis.values[0]['time'].to_i - reis.keys[0]['time'].to_i)
        mas_navi["ExportAtt"].each{|el|
          time_navi = DateTime.parse(Time.at(el["time"].to_i/1000).localtime($TimeZone).to_s)
          if (time_navi <= reis_end and time_navi >= reis_begin)
            reis_points_mas << [el['lat'].to_f, el['lon'].to_f]
          end
        }
        reis_points_mas.each_index{|index|
          unless (index + 1 == reis_points_mas.size)
          distances_mas << distance(reis_points_mas[index], reis_points_mas[index + 1])
          end
          }
        reis_length = distances_mas.inject(0, :+)
        speed = get_speed_distance(delta_t, reis_length)
        reis_string = "#{date}\t#{id_bnst}\t#{reis_begin.strftime('%H:%M:%S')}\t#{reis_end.strftime('%H:%M:%S')}\t#{reis_length.round(1).to_s.gsub('.',',')}\t#{speed.to_s.gsub('.',',')}"
        all_reises_mas << reis_string if reis_length <= (1.5*distance($loc1, $loc1_2)) # Заносим рейс в финальный список, если пройденное расстоянии не больше 150% от расстояния между двумя ОП
        bad_reises_mas << reis_string if reis_length > (1.5*distance($loc1, $loc1_2))
        }     
  rescue Exception => e
    puts "#{e}"
    puts "Нет данных по #{id_bnst}"
  end
    }
  }
stars = "####"*125 ;p
puts "\n\n#{stars}\n\n\nРейсы, прошедшие все фильтры:\nДата\tATT\tВремя начала рейса\tВремя окончания рейса\tПройденное расстояние\tСредняя скорость (км/ч)" ;p
all_reises_mas.each{|reis| puts reis}

(puts "\n\n#{stars}\nПлохие рейсы, для перепроверки:\nДата\tATT\tВремя начала рейса\tВремя окончания рейса\tПройденное расстояние\tСредняя скорость (км/ч)" ;p) if bad_reises_mas.size != 0
bad_reises_mas.each{|reis| puts reis}
