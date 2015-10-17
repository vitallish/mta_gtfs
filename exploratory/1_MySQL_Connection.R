require(RMySQL)
require(dplyr)
require(lubridate)
require(ggplot2)

load("dbConstants.RData")
drv<-dbDriver("MySQL")
con<-dbConnect(drv,
               username = USERNAME,
               password = PASSWORD,
               host= HOST,
               port = PORT,
               dbname = DBNAME)
dbListTables(con)

sched_stops <- tbl_df(dbReadTable(con, "sched_stops"))
trainID <- tbl_df(dbReadTable(con, "trainID"))
enroute_trains <- tbl_df(dbReadTable(con, "enroute_trains"))
stops <- tbl_df(dbReadTable(con, "stops"))
dbDisconnect(con)


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


