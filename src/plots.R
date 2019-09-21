
#### Plot for Pickups/Dropoffs / hour ####


pickupsperhour <- read.csv("results/pickupsperhour.csv")

dropoffsperhour <- read.csv("results/dropoffsperhour.csv")

plot(x = pickupsperhour$PU_hour, y = pickupsperhour$count/1000,
     type = "n", xlab = "Time of day", ylab = "x1000 Pick-Ups/Drop-offs",
     xlim= c(0,23), xaxp = c(0, 24, 24))
points(x = pickupsperhour$PU_hour, y = pickupsperhour$count/1000,
       type ="l",
       col = "red")
points(x = dropoffsperhour$DO_hour, y = dropoffsperhour$count/1000,
       type ="l",
       col = "blue")
legend(15, 80, legend = c("Pick-Ups", "Dropoffs"), fill=c("red","blue"))

#### Plot for Fare Variations / hour ####

fareperhour <- read.csv("results/fareperhour.csv")
plot(x = fareperhour$PU_hour, y = fareperhour$avg.total_amount.,
     type = "b", xlab = "Time of day at pickup", ylab = "Total amount in USD",
     xlim= c(0,23), xaxp = c(0, 24, 24))

#Correlation price-rides taken
cor(fareperhour$avg.total_amount., pickupsperhour$count)

#### Plot for Fare Variations / year ####


fareperday <- read_csv("results/fareperday.csv", 
                       col_types = cols(PU_day = col_date(format = "%Y-%m-%d")))


#select only those days that lie in the area of interest
fareperday <- fareperday[fareperday$PU_day >= "2018-01-01"
                         & fareperday$PU_day <= "2018-06-30",]


plot(fareperday, type = "l", lty=3, xlab = "Day", ylab = "Fare in USD")
lines(y = filter(fareperday$`avg(total_amount)`,rep(1/7,7)),
      x = fareperday$PU_day,
      col = "red", type = "l", lwd=2)
legend(x=as.Date("2018-04-15"), y = 14.5, legend = c("Price per day", "7-day avg"),
       lty = c(3,1), lwd= c(1,2),
       col=c("black","red"))


# Plot Fare changes during the week
fareperday$weekday = (1:length(fareperweek$PU_day) -1) %% 7

fareperweekday <- aggregate(fareperday, by = list(fareperday$weekday), FUN = mean)
plot(fareperweekday$weekday, fareperweekday$`avg(total_amount)`, type = "b",
     xlab = "Weekday", xaxt= "n", ylab = "Fare in USD")
axis(1, at= 0:6, labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))


#### Scatterplot Distance / Fare ####


library(readr)
test_dataframe <- read_csv("test_dataframe.csv")
View(test_dataframe)

plot(test_dataframe$fare_amount, test_dataframe$trip_distance,
     pch = ".", xlim = c(0, 500), ylim = c(0,150),
     xlab = "Fare", ylab = "Distance")



##zoomed in

plot(test_dataframe$fare_amount, test_dataframe$trip_distance,
     pch = ".", xlim = c(0, 200), ylim = c(0,50),
     xlab = "Fare", ylab = "Distance")

