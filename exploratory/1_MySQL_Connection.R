require("RMySQL")
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

sched_stops_clean <- sched_stops %>% 
  mutate(arrival = ymd_hms(arrival), departure = ymd_hms(departure),
         timeFeed = ymd_hms(timeFeed))
trainID_clean <- trainID %>% 
  mutate(start_date  = ymd(start_date) )



full_df <- sched_stops %>% 
  left_join(stops, by ="stop_id") %>% 
  left_join(trainID_clean, by = 'full_id')

direct <- full_df %>% 
  filter(route_id == "1", direction == "S") %>% 
  group_by(full_id) %>% 
  arrange(desc(departure)) %>% 
  mutate(stop_num = row_number(stop_id)) %>% 
  #left_join(enroute_trains,by = c('full_id','timeFeed','stop_id','stop_name')) %>% 
  ggplot(data = ., aes(x = reorder(stop_name,order(stop_num)), y = departure))+
  geom_point() + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))
  
  




