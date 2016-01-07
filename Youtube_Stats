library(httr)
library(ggplot2)
library(scales)

user<-"MIT"
api<-" "

c_id<-content(GET(paste0("https://www.googleapis.com/youtube/v3/channels?key=", api, "&forUsername=", user ,"&part=id")))

id<-content(GET(paste0("https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=", c_id$items[[1]]$id, "&maxResults=50&order=date&key=", api)))
page<-id$nextPageToken
id<-id$items
for(iua in 1:10000){
id2<-content(GET(paste0("https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=", c_id$items[[1]]$id, "&maxResults=50&order=date&key=", api, "&pageToken=",page)))
page<-id2$nextPageToken
id<-append(id, id2$items)
if(length(id2$nextPageToken)==0){
  break;
}
}



video_title<-sapply(id, function(x) x$snippet$title)
video_id<-sapply(id, function(x) x$id$videoId)
video_time<-sapply(id, function(x) x$snippet$publishedAt)

for(i in 1:length(video_id)){
  if(length(video_id[[i]])==0){
    video_time[[i]]<-"hello"
    video_title[[i]]<-"hello"
  }
}

video_time<-video_time[video_time!="hello"]
video_title<-video_title[video_title!="hello"]
vid<-unlist(video_id)

df<-as.data.frame(vid)
names(df)<-"id"
df$title<-video_title
df$time<-video_time
df$time<-gsub("T", " ", df$time)
df$time<-gsub("Z", "", df$time)
df$time<-gsub(".000", "", df$time)
df$time<-as.POSIXct(strptime(df$time, "%Y-%m-%d %H:%M:%S"))


stat<-c()
x<-1
y<-50
for(g in 1:((round(length(vid)/50, 0))+1)){
video_id2<-paste(vid[x:y], sep="", collapse=",")
stat<-append(stat, (content(GET(paste0("https://www.googleapis.com/youtube/v3/videos?part=contentDetails,statistics&id=", video_id2 ,"&key=", api))))$items)
x<-x+50
y<-y+50
}

df$duration<-sapply(stat, function(x) x$contentDetails$duration)
df$views<-sapply(stat, function(x) x$statistics$viewCount)
df$likes<-sapply(stat, function(x) x$statistics$likeCount)
df$dislikes<-sapply(stat, function(x) x$statistics$dislikeCount)
df$comments<-sapply(stat, function(x) x$statistics$commentCount)

df$duration<-gsub("PT", "", df$duration)

df[!(grepl("S", df$duration)),]$duration<-paste0(df[!(grepl("S", df$duration)),]$duration, "0S")
df[!(grepl("M", df$duration)),]$duration<-paste0(df[!(grepl("M", df$duration)),]$duration, "0M")
df[!(grepl("H", df$duration)),]$duration<-paste0(df[!(grepl("H", df$duration)),]$duration, "0H")

df$duration<-gsub("H", "*3600,", df$duration)
df$duration<-gsub("M", "*60,", df$duration)
df$duration<-gsub("S", "*1,", df$duration)

x<-strsplit(df$duration, ",")

vec<-c();
for(i in x){
p<-paste0(i[1], "+", i[2], "+", i[3]) 
vec<-append(vec, eval(parse(text=p)))
}

df$duration<-vec
ggplot(df, aes(x=as.Date(time), y=as.numeric(views)))+geom_line()+ scale_y_continuous(breaks = pretty_breaks(n = 15), labels=comma)+theme_bw()+ geom_hline(yintercept=mean(as.numeric(df$views)),color='red', linetype = 2, size=1.25)+ geom_hline(yintercept=median(as.numeric(df$views)),color='blue', linetype = 2, size=1.25)
