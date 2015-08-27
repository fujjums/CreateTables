
rm(list = ls())
library(dplyr)
library(WriteXLS)
library(stringr)
library(tidyr)

setwd("/Users/ShoFarmigo/Dropbox/Farmigo/data/CreateTables")
Orders <- as.data.frame(read.csv("15-08-27 - Orders (includes closed comms).csv", stringsAsFactors=FALSE))
DateswithWeeks <- as.data.frame(read.csv("Dates.csv"))

#change dates from strings-> dates
Orders$First.order <- as.Date(Orders$First.order, format = "%m/%d/%y")
Orders$Last.order <- as.Date(Orders$Last.order, format = "%m/%d/%y")
Orders$Pick.up.Date <- as.Date(Orders$Pick.up.Date, format = "%m/%d/%y")
DateswithWeeks$date <- as.Date(DateswithWeeks$date, format = "%m/%d/%Y")
DateswithWeeks$Week <- as.Date(DateswithWeeks$Week, format = "%m/%d/%Y")

#Modify Orders Table
#Create Weeks fields for all dates
#InnerJoin (like vlookup) Dates table (with weeks) to Orders Table. Merge can be used to, but merge rearranges columns
Orders <- inner_join(Orders, DateswithWeeks, by = c("Pick.up.Date" = "date"))
Orders <- rename(Orders, order_week = Week)
Orders <- inner_join(Orders, DateswithWeeks, by = c("First.order" = "date"))
Orders <- rename(Orders, mem_first_order_week = Week)
Orders <- inner_join(Orders, DateswithWeeks, by = c("Last.order" = "date"))
Orders <- rename(Orders, mem_last_order_week = Week)

#perfectorder?
Orders$PerfectOrder <- ifelse(is.na(Orders$Comp) & Orders$Shorts == "No", "Yes", "No")



#Create Community Table
Community <- Orders %>%
  group_by(Community, Community.Id, Region, Community.Type) %>% 
  summarise(comm_first_pickup_date = min(Pick.up.Date), 
            comm_last_pickup_date = max(Pick.up.Date), 
            comm_total_pickups = max(Community.Pickup.Week),
            distinct_members = n_distinct(Subscription.Id))

#Create Weeks fields for all dates
Community <- inner_join(Community, DateswithWeeks, by = c("comm_first_pickup_date" = "date"))
Community <- rename(Community, comm_first_pickup_week = Week)
Community <- inner_join(Community, DateswithWeeks, by = c("comm_last_pickup_date" = "date"))
Community <- rename(Community, comm_last_order_week = Week)

CommunityLTV <- Orders %>%
  group_by(Community.Id) %>%
  summarize(
    distinctmembersfirst12 =sum(Member.Order.Number==1 & Community.Pickup.Week <= 12),
    sum12 = sum(Value[Community.Pickup.Week<=12], na.rm=TRUE),
    sum24 = sum(Value[Community.Pickup.Week<=24], na.rm=TRUE),
    sum48 = sum(Value[Community.Pickup.Week<=48], na.rm=TRUE),
    count12 = sum(Community.Pickup.Week<=12, na.rm=TRUE),
    count24 = sum(Community.Pickup.Week<=24, na.rm=TRUE),
    count48 = sum(Community.Pickup.Week<=48, na.rm=TRUE))

Community <- inner_join(Community, CommunityLTV, by = c("Community.Id" = "Community.Id")) 

Community <- transform(Community, 
                       sum12 = ifelse(comm_total_pickups   <= 12, NA, sum12),
                       sum24 = ifelse(comm_total_pickups   <= 24, NA, sum24),
                       sum48 = ifelse(comm_total_pickups   <= 48,NA , sum48),
                       count12 = ifelse(comm_total_pickups   <= 12,NA , count12),
                       count24 = ifelse(comm_total_pickups   <= 24,NA , count24),
                       count48 = ifelse(comm_total_pickups   <= 48,NA , count48))          

#Create Members Table
Members <- Orders %>%
  group_by(Community, Subscription.Id, Community.Id, Region, Community.Type, X..Orders,  X..Sales, First.order, Last.order) %>%
  summarise(mem_first_pickup = min(Community.Pickup.Week), mem_last_pickup = max(Community.Pickup.Week))

#Create Weeks fields for all dates
Members <- inner_join(Members, DateswithWeeks, by = c("First.order" = "date"))
Members <- rename(Members, mem_first_order_week = Week)
Members <- inner_join(Members, DateswithWeeks, by = c("Last.order" = "date"))
Members <- rename(Members, mem_last_order_week = Week)

#Merge (like vlookup) Community comm_total_pickups to Members table (To calculate lapsed members)
Members <- merge(Members, Community[, c("Community.Id", "comm_total_pickups", "comm_last_order_week")], by.x = "Community.Id", by.y = "Community.Id")



#Calculate the number of weeks that they've lapsed (if any)
Members <- Members %>%
  mutate(PickupsLapsed = comm_total_pickups - mem_last_pickup) %>%
  mutate(mem_total_pickups = comm_total_pickups - mem_first_pickup + 1)

#Calculate whether they can be officially lapsed (>5 lapsed weeks) and if they're newly lapsed (6th week of lapse)
Members$Lapsed_Bool <- ifelse(Members$PickupsLapsed > 4, 1, 0)
Members$NewlyLapsed_Bool <- ifelse(Members$PickupsLapsed == 5, 1, 0)


#Calculate sum of pick-up values for pickups up to 12, 24, and 48th pickup (for LTV calc)
# The second part (transform) removes sum calculations for all members that haven't hit those milestones
MemberLTV <- Orders %>%
  group_by(Subscription.Id) %>%
  summarize(
    sum4 = sum(Value[Member.Pick.up.Week<=4], na.rm=TRUE),
    sum12 = sum(Value[Member.Pick.up.Week<=12], na.rm=TRUE),
    sum24 = sum(Value[Member.Pick.up.Week<=24], na.rm=TRUE),
    sum48 = sum(Value[Member.Pick.up.Week<=48], na.rm=TRUE),
    count2 = sum(Member.Pick.up.Week<=2, na.rm=TRUE),
    count4 = sum(Member.Pick.up.Week<=4, na.rm=TRUE),
    count12 = sum(Member.Pick.up.Week<=12, na.rm=TRUE),
    count24 = sum(Member.Pick.up.Week<=24, na.rm=TRUE),
    count48 = sum(Member.Pick.up.Week<=48, na.rm=TRUE))

Member2ndOrder <- Orders %>%
  filter(Member.Order.Number == 2) %>%
  group_by(Subscription.Id) %>%
  mutate(ThreeWeeks = mem_first_order_week + 21) %>%
  summarize(sum(order_week <= ThreeWeeks))


Members <- inner_join(Members, MemberLTV, by = c("Subscription.Id" = "Subscription.Id")) 

Members <- transform(Members, 
                     sum4 = ifelse(mem_total_pickups   <= 4, NA, sum4),
                     sum12 = ifelse(mem_total_pickups   <= 12, NA, sum12),
                     sum24 = ifelse(mem_total_pickups   <= 24, NA, sum24),
                     sum48 = ifelse(mem_total_pickups   <= 48, NA, sum48),
                     count2 = ifelse(mem_total_pickups   <= 2, NA, count2),
                     count4 = ifelse(mem_total_pickups   <= 4, NA, count4),
                     count12 = ifelse(mem_total_pickups   <= 12, NA, count12),
                     count24 = ifelse(mem_total_pickups   <= 24, NA, count24),
                     count48 = ifelse(mem_total_pickups   <= 48, NA, count48))          
Members <- left_join(Members, Member2ndOrder, by = c("Subscription.Id" = "Subscription.Id"))



#Create CommunityWeeks
CommunityWeeks <- Orders %>%
  group_by(Community, Community.Id, Community.Pickup.Week, Pick.up.Date, order_week, Region, Community.Type) %>% 
  summarise(order_count = n(),
            order_value = sum(Value),
            AOV = order_value/order_count)



#For each Community.Pickup.Week, group orders by nth member order
counts <- Orders %>%
  group_by(Community.Id, Community.Pickup.Week, Member.Order.Number) %>%
  tally
counts <- counts %>% spread(Member.Order.Number, n, fill = 0)

counts <- data.frame(counts[,1:2],
                     Ordered1st = rowSums(counts[,c(3)]),
                     Ordered2nd3rd = rowSums(counts[,c(4,5)]),
                     Ordered4th5th = rowSums(counts[,c(6,7)]),
                     Ordered6th9th = rowSums(counts[,c(8:11)]),
                     Ordered10thplus = rowSums(counts[,c(12:ncol(counts))]))  

CommunityWeeks <- left_join(CommunityWeeks, counts, by = c("Community.Id", "Community.Pickup.Week"))

#count mem_first_pickups by community (tells us how many members were new for a specific pick-up)
counts2 <- Members %>%
  group_by(Community.Id, mem_first_pickup) %>%
  tally %>%
  rename(NewMembers = n)

CommunityWeeks <- left_join(CommunityWeeks, counts2, by = c("Community.Id", "Community.Pickup.Week" = "mem_first_pickup"))

#create count data.frame to calculate lapse week for every member, and then count the 
#number of members for each community/pick-up combo that lapsed, by total order count
#http://stackoverflow.com/questions/29773401/count-based-on-multiple-conditions-from-other-data-frame
counts3 <- Members %>%
  mutate(lapsed_pickup = mem_last_pickup + 5) %>%
  group_by(Community.Id, lapsed_pickup, X..Orders ) %>%
  tally
counts3 <- counts3 %>% spread(X..Orders, n, fill = 0)

#Group lapsed community/pick-up/total order count by range
counts3 <- data.frame(counts3[,1:2],
                      Lapsed1Order = rowSums(counts3[,c(3)]),
                      Lapsed2to3Orders = rowSums(counts3[,c(4,5)]),
                      Lapsed4to5Orders = rowSums(counts3[,c(6,7)]),
                      Lapsed6to9Orders = rowSums(counts3[,c(8:11)]),
                      Lapsed10plusOrders = rowSums(counts3[,c(12:ncol(counts3))]))

CommunityWeeks <- left_join(CommunityWeeks, counts3, by = c("Community.Id", "Community.Pickup.Week" = "lapsed_pickup"))


CommunityWeeks[is.na(CommunityWeeks)] <- 0 # convert  all NAs to 0s

#Calculate Total members that lapsed in Community/Pick-up
CommunityWeeks<- CommunityWeeks %>%
  mutate(TotalLapsed = Lapsed1Order+ Lapsed2to3Orders + Lapsed4to5Orders + Lapsed6to9Orders + Lapsed10plusOrders)

#Calculate Total ActiveMembesr in Community/Pickup
CommunityWeeks <- CommunityWeeks %>%
  group_by(Community.Id) %>%
  mutate(TotalMembers = cumsum(NewMembers)-cumsum(TotalLapsed))

#Create OrderPercentage table -> the % of people in a weekly cohort that made an nth order
Ordercounts <- Orders %>%
  group_by(mem_first_order_week, Member.Order.Number) %>%
  tally
Ordercounts <- Ordercounts %>% spread(Member.Order.Number, n, fill = 0)
OrderPercentage <- (function(x) x/x[[1]] )(Ordercounts[,-1])
Ordercounts <- data.frame(Ordercounts[,1], OrderPercentage)



#If a member starts ordering from a new community, the system treats them like a new customer.
#except that they have the same Subscription.ID. Everything else is different. 
#CommunityNumber tells us which Community a member's order comes from 
CommunityNumber <- Orders %>% 
  group_by(Subscription.Id, Community.Id, mem_first_order_week) %>%
  summarise() %>%
  group_by(Subscription.Id) %>%
  arrange(mem_first_order_week) %>%
  mutate(CommunityNumber=seq(n())) 

Orders <- inner_join(Orders, CommunityNumber, by=c("Community.Id", "Subscription.Id", "mem_first_order_week"))


#write.csv(Ordercounts, file="Ordercounts.csv", row.names=FALSE, quote = FALSE) 
write.csv(CommunityWeeks, file="toMergeCommunityWeeks.csv", row.names=FALSE, quote = FALSE) 
write.csv(Community, file="toMergeCommunity.csv", row.names=FALSE, quote = FALSE) 
write.csv(Members, file="toMergeMembers.csv", row.names=FALSE, quote = FALSE) 
write.csv(Orders, file="toMergeOrders.csv", row.names=FALSE, quote = FALSE) 
