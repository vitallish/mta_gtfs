 
require(RMySQL)
require(dplyr)
require(lubridate)
require(ggplot2)
library(leaflet)
library(htmltools)



drv<-dbDriver("MySQL")
con<-dbConnect(drv,
               username = db_user,
               password = db_pass,
               host= db_host,
               port = db_port,
               dbname = db_table)
dbListTables(con)

sched_stops <- tbl(con, "sched_stops")
trainID <- tbl(con, "trainID")
enroute_trains <- tbl(con, "enroute_trains")
stops <- tbl(con, "stops")


## Live information ----
for(i in 1:4){
cur_time <- Sys.time()
live_query <- sched_stops %>% 
  left_join(trainID, by = "full_id") %>% 
  left_join(stops, by = "stop_id") %>% 
  inner_join(enroute_trains, by = "full_id", suffix = c("", "_enroute")) %>% 
  # left_join(enroute_trains, by = c("full_id", "stop_id")) %>% 
  filter(route_id == "1", direction == "S") %>% 
  select(-full_stop_id)


live_df <- live_query %>%
  collect() 

live_clean <- live_df %>% 
  mutate_at(vars(arrival, departure, timeFeed, last_ping), ymd_hms, tz =  Sys.timezone()) %>% 
  mutate(cur_time = Sys.time())

current_train_pos <- live_clean %>% 
  group_by(full_id) %>% 
  mutate(time_pinged = cur_time -last_ping) %>% 
  mutate(time_since_depart = as.numeric(cur_time - lag(departure, order_by = stop_sequence), unit = "mins")) %>% 
  mutate(arriving_in = as.numeric(arrival - cur_time, unit = "mins")) %>% 
  mutate(perc_there = case_when(current_status == 'STOPPED_AT' ~ 1,
                                TRUE ~ time_since_depart/(time_since_depart + arriving_in))) %>% 
  mutate(guess_lat = lag(stop_lat, order_by = stop_sequence) + (stop_lat - lag(stop_lat, order_by = stop_sequence))*(perc_there)) %>% 
  mutate(guess_lon = lag(stop_lon, order_by = stop_sequence) + (stop_lon - lag(stop_lon, order_by = stop_sequence))*(perc_there)) %>% 
  
  filter(stop_id_enroute == stop_id) %>% 
  mutate(PINGED = paste0("PINGED: ", round(as.numeric(cur_time-last_ping, unit = "secs")))) %>% 
  mutate(ARRIVAL = paste0("Arrival: ", round(arriving_in, 1), " min")) %>% 
  mutate(LEFT = paste0("Left: ", round(time_since_depart,1), " min")) %>% 
  mutate(station_color = case_when(current_status == "STOPPED_AT" ~ "green",
                                   current_status == "INCOMING_AT" ~ "yellow",
                                   TRUE ~ "blue"))

all_stations <- live_clean %>% 
  distinct(stop_name, stop_id, stop_lat, stop_lon)




output <- leaflet() %>% 
  addCircleMarkers(~stop_lon, ~stop_lat, data = all_stations, radius = 2, color = "black", popup = ~htmlEscape(stop_name)) %>%  
  addProviderTiles(providers$CartoDB.Positron) %>% 
  addCircleMarkers(~guess_lon, ~guess_lat, data = current_train_pos, color = ~station_color, popup = ~(
    paste(PINGED, 
          paste0(current_status, ": ", stop_name),
          ARRIVAL,
          LEFT,
          
          sep = "<br>")
  ))

print(output)
Sys.sleep(15)

}

## Historic Information ----
test_query <- sched_stops %>% 
  left_join(trainID, by = "full_id") %>% 
  left_join(stops, by = "stop_id") %>% 
  filter(route_id == "1", direction == "S", enroute_conf > 0) %>% 
  select(-full_stop_id)
  

test_query_df <- test_query %>% 
    collect()



d <- test_query_df %>% 
  mutate_at(vars(arrival, departure, timeFeed), ymd_hms) %>% 
  group_by(full_id) %>% 
  arrange(full_id, desc(arrival)) %>% 
  mutate(time_to_arrive = arrival-lag(departure, order_by = (arrival)))
  


d %>% 
  filter(stop_id %in% c('114S', '127S')) %>% 
  mutate(time_to_arrive = arrival-lag(departure, order_by = (arrival))) %>% 
  filter(!is.na(time_to_arrive)) %>% 
  filter(year(arrival) >=2019) %>% 
  filter(time_to_arrive < 5e3) %>% 
  ggplot(aes(x = hour(arrival), y = as.numeric(time_to_arrive)/60)) +
  geom_boxplot(aes(group = hour(arrival)))



sched_stops_clean <- sched_stops %>% 
  mutate(arrival = ymd_hms(arrival), departure = ymd_hms(departure),
         timeFeed = ymd_hms(timeFeed))

sched_stops_clean$departure[sched_stops_clean$departure<0] <-NA
sched_stops_clean$arrival[sched_stops_clean$arrival<0] <-NA

trainID_clean <- trainID %>% 
  mutate(start_date  = ymd(start_date) )

full_df <- sched_stops_clean %>% 
  left_join(stops, by ="stop_id") %>% 
  left_join(trainID_clean, by = 'full_id') %>% 
  filter(enroute_conf != 0)

# filter_options needs to be loaded into a db and updated periodically
filter_options <- full_df %>% 
  select(route_id, direction, stop_id, stop_name) %>%
  distinct(route_id, direction, stop_id, stop_name)
  

full_df_all<- sched_stops_clean %>% 
  left_join(stops, by ="stop_id") %>% 
  left_join(trainID_clean, by = 'full_id')

trip_in_question <- full_df %>% 
  filter(route_id == "1", direction == "S")

trip_begin <- trip_in_question %>% 
  filter(stop_name == "96 St") %>% 
  select(full_id, station_leave_time = departure) %>% 
  mutate(station_leave_hour = hour(station_leave_time)) %>% 
  mutate(station_leave_cut = cut(station_leave_hour,c(-.1,7,9,17,19,24)))
  
full_travel_time <- trip_in_question %>% 
  inner_join(trip_begin, by='full_id') %>% 
  mutate(time_in_transit = arrival - station_leave_time) %>% 
  group_by(full_id) %>% 
  arrange(timeFeed, enroute_conf)

stop_med <- full_travel_time %>% group_by(stop_name) %>% 
  summarise(med_travel = median(time_in_transit,na.rm = T)) %>%
  arrange(as.numeric(med_travel)) %>% 
  mutate(stop_num = row_number(stop_name))
  
full_travel_time %>%  
  ggplot(data = ., aes(x = stop_name, y = as.numeric(time_in_transit), 
                       color = station_leave_cut))+
  geom_point(position = "jitter", alpha = 0.2) +
  stat_summary(fun.y=mean, geom = "point", size = 4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  scale_x_discrete(limits = stop_med$stop_name)
 
full_travel_time %>%
  filter(stop_name == "Chambers St") %>% 
  group_by(station_leave_cut) %>% 
  select(full_id, arrival, departure, time_in_transit) %>% 
  summarise(avg_travel = mean(as.numeric(time_in_transit))/60, n())

full_travel_time %>%
  filter(stop_name == "Chambers St") %>% 
  ggplot(data = ., aes(x = as.numeric(time_in_transit))) +
  geom_density(alpha = 0.2)


getNextTrainsatStop <-function(enroute_df, full_df_all, stop_interest, 
                               train_lines, dir_interest, num_trains, 
                               time_start = force_tz(now(),tzone = "UTC")){
  train_ids<- enroute_df$full_id
  full_df_all %>% 
    filter(stop_name == stop_interest,
           full_id %in% train_ids,
           route_id %in% train_lines, 
           departure > time_start,
           direction == dir_interest) %>%
    top_n(num_trains,departure)
}

# figure out how long between trains

getTimeBetweenTrains <-function(full_df, stop_interest, 
                                train_lines, dir_interest){
  
  full_df %>% 
    filter(stop_name == stop_interest,
           route_id %in% train_lines, 
           direction == dir_interest) %>%
    arrange(departure) %>% 
    mutate(time_between = lead(departure)-departure)
}

#density by hour, train time wait
OK <- getTimeBetweenTrains(full_df, "96 St", c("1"),"S") %>% filter(time_between<5000) %>% mutate(time_between = as.numeric(time_between))
OK %>% mutate(station_leave_hour = hour(departure)) %>% mutate(station_leave_cut = cut(station_leave_hour,c(-.1,7,9,17,19,24))) %>% qplot(time_between, data = ., color = station_leave_cut,geom = "density")

qplot(data = OK, x = arrival, y = time_between, color = hour(arrival))
#time difference between different trains, for example: time difference between 1 train arrival and 2/3 train departure

getTimeTransfer <- function(full_df, arriving_trains = "1", transfer_trains = c("2","3"), dir = "S", 
                            transfer_stop = "96 St", minimum_time  = seconds(0)){
  trains_of_interest <- full_df %>% 
    filter(route_id %in% c(arriving_trains, transfer_trains), 
           stop_name == transfer_stop, direction == dir) %>% 
    mutate(toi =ifelse(route_id %in% arriving_trains,arrival,departure)) %>%
    mutate(arriving_bool = ifelse(route_id %in% arriving_trains, T, F)) %>% 
    arrange(toi) %>% 
    mutate(time_wait = NA)
  
  
  
  
  for(row in 1:nrow(trains_of_interest)){
    spec_row <- trains_of_interest[row,]
    if(spec_row$arriving_bool){
      temp_row_num <- row
      while(trains_of_interest$arriving_bool[temp_row_num]){
        temp_row_num = temp_row_num + 1
      }
      trains_of_interest$time_wait[row] <- 
        trains_of_interest$toi[temp_row_num] - spec_row$toi
      
    }
    
  }
  
  trains_of_interest
  
}

getTimeTransfer(full_df) %>% filter(!is.na(time_wait)) %>% qplot(data = ., x = arrival, y = time_wait, color = hour(arrival))


