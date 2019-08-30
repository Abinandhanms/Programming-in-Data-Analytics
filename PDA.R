setwd("D:/MS/Trisemester-2/PDA/Final project/Scripts/Cleaning")

data<-read.csv("employee_reviews.csv",na.strings = TRUE,stringsAsFactors = FALSE)
#str(data)
data$company<-as.factor(data$company)
library(dplyr)
data = select(data,-location)
data = select(data,-dates)
data = select(data, -link)
data = select(data, -helpful.count)
data = select(data, -advice.to.mgmt)

#removing NONE values to N/A in work.balance.stars 
#unique(data$work.balance.stars)
data$work.balance.stars[data$work.balance.stars =="none"] <- " "
data$work.balance.stars<-as.numeric(data$work.balance.stars)

#remving NONE values to N/A in culture.values.stars
#unique(data$culture.values.stars)
data$culture.values.stars[data$culture.values.stars =="none"] <- " "
data$culture.values.stars<-as.numeric(data$culture.values.stars)

#remving NONE values to N/A in  carrer.opportunities.stars
#unique(data$carrer.opportunities.stars)
data$carrer.opportunities.stars[data$carrer.opportunities.stars =="none"] <- " "
data$carrer.opportunities.stars<-as.numeric(data$carrer.opportunities.stars)

#remving NONE values to N/A in  comp.benefit.stars
#unique(data$comp.benefit.stars)
data$comp.benefit.stars[data$comp.benefit.stars =="none"] <- " "
data$comp.benefit.stars<-as.numeric(data$comp.benefit.stars)

#remving NONE values to N/A in  senior.mangemnet.stars
#unique(data$senior.mangemnet.stars)
data$senior.mangemnet.stars[data$senior.mangemnet.stars =="none"] <- " "
data$senior.mangemnet.stars<-as.numeric(data$senior.mangemnet.stars)

#removing N/A from data 
sapply(data, function(x) {sum(is.na(x))})
#unique(data$overall.ratings)

xMean<-mean(data$work.balance.stars, na.rm = TRUE)
xMean<-as.numeric(xMean)
xMean<-round(xMean,digit=1)
data$work.balance.stars<-ifelse(is.na(data$work.balance.stars), xMean, data$work.balance.stars)
sapply(data, function(x) {sum(is.na(x))})

xMean<-mean(data$culture.values.stars, na.rm = TRUE)
xMean<-as.numeric(xMean)
xMean<-round(xMean,digit=1)
data$culture.values.stars<-ifelse(is.na(data$culture.values.stars), xMean, data$culture.values.stars)
sapply(data, function(x) {sum(is.na(x))})

xMean<-mean(data$carrer.opportunities.stars, na.rm = TRUE)
xMean<-as.numeric(xMean)
xMean<-round(xMean,digit=1)
data$carrer.opportunities.stars<-ifelse(is.na(data$carrer.opportunities.stars), xMean, data$carrer.opportunities.stars)
sapply(data, function(x) {sum(is.na(x))})

xMean<-mean(data$comp.benefit.stars, na.rm = TRUE)
xMean<-as.numeric(xMean)
xMean<-round(xMean,digit=1)
data$comp.benefit.stars<-ifelse(is.na(data$comp.benefit.stars), xMean, data$comp.benefit.stars)
sapply(data, function(x) {sum(is.na(x))})

xMean<-mean(data$senior.mangemnet.stars, na.rm = TRUE)
xMean<-as.numeric(xMean)
xMean<-round(xMean,digit=1)
data$senior.mangemnet.stars<-ifelse(is.na(data$senior.mangemnet.stars), xMean, data$senior.mangemnet.stars)
sapply(data, function(x) {sum(is.na(x))})

#unique(data$company)
#names(data)
#chaning colnames
names(data)[1]<-"SNo"
names(data)[3]<-"job_title"
names(data)[7]<-"overall_ratings"
names(data)[8]<-"workLifebalance_ratings"
names(data)[9]<-"culturevalues_ratings"
names(data)[10]<-"carreropportunities_ratings"
names(data)[11]<-"benefits_ratings"
names(data)[12]<-"seniormangemnet_ratings"
rm(xMean)

#str(data)
reviwerType1 <- as.data.frame(substr(data$job_title,0,18))
names(reviwerType1)[1]<-"ReviwerType"
data<-cbind(data,reviwerType1)
data$job_title<-gsub("Current Employee - "," ",data$job_title)
data$job_title<-gsub("Former Employee - "," ",data$job_title)
data$ReviwerType<-as.character(data$ReviwerType)
rm(reviwerType1)
data$ReviwerType[data$ReviwerType =="Current Employee -"] <- "Employee"
data$ReviwerType[data$ReviwerType =="Former Employee - "] <- "Alumni"

#names(data)
data<-data[c("SNo","company","ReviwerType","job_title","summary","pros","cons","overall_ratings","workLifebalance_ratings",
             "culturevalues_ratings","carreropportunities_ratings","benefits_ratings","seniormangemnet_ratings")]

data = select(data,-summary)
data$workLifebalance_ratings<-round(data$workLifebalance_ratings,digit=0)
data$culturevalues_ratings<-round(data$culturevalues_ratings,digit=0)
data$carreropportunities_ratings<-round(data$carreropportunities_ratings,digit=0)
data$benefits_ratings<-round(data$benefits_ratings,digit=0)
data$seniormangemnet_ratings<-round(data$seniormangemnet_ratings,digit=0)

data$pros<-gsub("- "," ",data$pros)
data$cons<-gsub("- "," ",data$cons)

data$cons<-gsub("-"," ",data$cons)

data$Comments<-paste0(data$pros,". However, ",data$cons)
data = select(data, -pros)
data = select(data,-cons)

data<-data[c("SNo","company","ReviwerType","job_title","Comments","overall_ratings","workLifebalance_ratings",
             "culturevalues_ratings","carreropportunities_ratings","benefits_ratings","seniormangemnet_ratings")]

data$Comments<-gsub("[[:punct:]]", "", data$Comments)

#write.csv(data,file = "Review.csv",row.names = FALSE)

#Splitting into two files PIG and HDFS

PIG <- as.data.frame(select(data,-Comments))

Hdfs <- as.data.frame(select(data,SNo,Comments))

write.table(Hdfs, file = "Reviews.txt", sep = ",",row.names = FALSE)
write.table(PIG, file = "PIG.txt", sep = ",",row.names = FALSE)

#library(sqldf)
#sqldf('select * from data where job_title="Singapore"')
