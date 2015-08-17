tryCatch(library(jsonlite), error = function(cond) { install.packages("jsonlite") 
                                                      library(jsonlite)}) #install JSON parsing package if not already installed
tryCatch(library(httr), error = function(cond) { install.packages("httr") 
                                                      library(httr)}) #install url parsing package if not already installed
tryCatch(library(reshape), error = function(cond) { install.packages("reshape") 
                                                      library(reshape)}) # install data manipulating package if not already installed
tryCatch(library(plyr), error = function(cond) { install.packages("plyr")
                                                      library(plyr)}) # install another data manipulating package if not already installed
tryCatch(library(redshift), error = function(cond) { install.packages("redshift") 
                                                      library(redshift)}) #Install redshift SQL query package if not already installed

conn <- redshift.connect("jdbc:postgresql://dw.c0ombftnueor.us-east-1.redshift.amazonaws.com:5439/ad", "username", "pasword") # Connection to local area Redshift, username and password ommitted

df <- dbGetQuery(conn, paste("SELECT id, dev_ua,  dev_browser, page_tree", 
                               "FROM ads_raw",
                               "WHERE cli_id = '4340340ed81f30ad392b83735a07f177a8f61867' AND has_page_tree IS TRUE AND frm_pos = 0 AND date(ts)> getdate() - INTERVAL '7 days'",
                               "GROUP BY 1,2,3,4",
                               "ORDER BY 1 DESC")) # PSQL query to extract JSON pagetree Info, and its attributes

tree<-gsub("\\\\", "", df$page_tree) # prepare JSON tree for parsing by deleting any string escape variables (\)

parsed_json <- function (tree) { #tree parsing function, wrapped in trycatch to avoid errors
  return(tryCatch(fromJSON(tree), error=function(e) NULL))
}
json_parsed<-lapply(tree, parsed_json) #lapply is equivalent to a for loop in others. Apply means to apply a function, in this case the parsed_url function the L in lapply means list, so it means to apply a function to every element in a list

frame_url=function(i){
  return((json_parsed[[i]]$loc)) # extract the frame url from the parsed JSON it will be under the loc header
}

url_frames<-lapply(1:length(json_parsed), frame_url) # see above
df$frame_url<-url_frames # attach the frame url to the original data frame

host=function(i){
  tryCatch({
    vec00<-parse_url(url_frames[[i]]) # using the url parser it loops through all frame urls and extracts the hostname
    return(vec00$hostname) # returns hostname
  }, error=function(e) {"NULL"})
}

path1=function(i){
  tryCatch({
    vec00<-parse_url(url_frames[[i]]) # SEE ABOVE
    return(vec00$path)
  }, error=function(e) {"NULL"})
}

query1=function(i){
  tryCatch({
    vec00<-parse_url(url_frames[[i]]) # SEE ABOVE
    return(vec00$query)
  }, error=function(e) {"NULL"})
}

frame_hostname<-lapply(1:length(url_frames), host) #See above
frame_pathname<-lapply(1:length(url_frames), path1) #after changing the function and running it to now extract path names loop through everything and extract the path
frame_query<-lapply(1:length(url_frames), query1) # same as above but for query string
df$frame_hostname<-frame_hostname # new column to dataframe called hostname which is the hostname to the frame url
df$frame_pathname<-frame_pathname # see above
df$frame_query<-frame_query # see above


#####################################################BEACONS###############################################################################################################################################################

df2<-df # copy the original dataframe into df2 so we can use it seperatly for scripts

beacons=function(i){
  return((json_parsed[[i]]$b)) # extracts all beacon urls
}
beacon_url<-lapply(1:(length(json_parsed)), beacons) # loops through list and extracts all beacon urls

split=function(i){
  return(beacon_url[i][[1]][1]) # WARNING MANUAL WORK, pulls beacon urls for each position, so this example is for the first beacon, you need to change this the last index ([1]) to [2] to get the second and then [3] for third etc.
}
bec1<-lapply(1:(length(beacons)), split) # extract all beacon urls for position 1
bec2<-lapply(1:(length(beacons)), split) # for position 2
bec3<-lapply(1:(length(beacons)), split) # for position 3

df$beacon1<-as.character(bec1) # add new column called beacon 1 to display only beacon urls in position 1
df$beacon2<-as.character(bec2) # add new column called beacon 1 to display only beacon urls in position 2
df$beacon3<-as.character(bec3) # add new column called beacon 1 to display only beacon urls in position 3

#if you need more just iterate the above process for 4 or 5 or 6

molten_pg_tree_b<-melt(df, id=c("id", "dev_ua", "dev_browser", "tree", "frame_url", "frame_hostname", "frame_pathname", "frame_query")) # use the melt function to convert the data frame from width wise to column wise, the id's in this function will repeat depending on the variables.

# Now we have a complete list of beacon urls with their corresponding ids, dev_ua, dev_browser etc attached

vec3<-c();
beacon_host=function(i){
  for(k in beacon_url[[i]]){ # from the list of urls we can extract the information we want
    temp_pg_tree<-parse_url(k)# parse through beacon tree
    vec3<-temp_pg_tree$hostname #return Hostname
  }
  return(vec3)
}

vec3<-c();
beacon_query1=function(i){ # SEE ABOVE
  for(k in beacon_url[[i]]){ 
    temp_pg_tree<-parse_url(k)
    vec3<-temp_pg_tree$query 
  }
  return(vec3)
}

beacon_hostname<-lapply(1:(length(beacon_url)), beacon_host) # see above 
beacon_query<-lapply(1:(length(beacon_url)), beacon_query1)

molten_pg_tree_b$pagetree_hostname<-as.character(beacon_hostname) #add new column to molted beacon data
molten_pg_tree_b$pagetree_query<-as.character(beacon_query)

type_b=function(i){ # from the list of beacon urls we can create a vector filled with the word beacon to add it to the type column
  return("beacons")
}
beacon_type<-lapply(1:(length(molten_pg_tree_b$value)), type_b) # see above
molten_pg_tree_b$type<-beacon_type # see above

#############################################SCRIPTS#############################################################################################

### THE FOLLOWING IS THE EXACT SAME AS THE BEACON WORK ABOVE EXCEPT IT HAS BEEN SWITCHED TO PULL SCRIPTS DATA INSTEAD

### SEE ABOVE

scripts=function(i){
  return((json_parsed[[i]]$s))
}
script_url<-lapply(1:(length(json_parsed)), scripts)

split=function(i){
  return(script_url[i][[1]][1])
}
sec1<-lapply(1:(length(scripts)), split)
sec2<-lapply(1:(length(scripts)), split)
sec3<-lapply(1:(length(scripts)), split)

df2$script1<-as.character(sec1)
df2$script2<-as.character(sec2)
df2$script3<-as.character(sec3)

molten_pg_tree_s<-melt(df2, id=c("id", "dev_ua", "dev_browser", "tree", "frame_url", "frame_hostname", "frame_pathname", "frame_query"))

vec5<-c();
script_host=function(i){
  for(k in script_url[[i]]){
    temp_pg_tree<-parse_url(k)
    vec5<-temp_pg_tree$hostname
  }
  return(vec5)
}

vec5<-c();
script_query1=function(i){
  for(k in script_url[[i]]){
    temp_pg_tree<-parse_url(k)
    vec5<-temp_pg_tree$query
  }
  return(vec5)
}

script_hostname<-lapply(1:(length(script_url)), script_host)
script_query<-lapply(1:(length(script_url)), script_query1)

molten_pg_tree_s$pagetree_hostname<-as.character(script_hostname)
molten_pg_tree_s$pagetree_query<-as.character(script_query)

type_s=function(i){
  return("scripts")
}
script_type<-lapply(1:(length(molten_pg_tree_s$value)), type_s)
molten_pg_tree_s$type<-script_type

total_data<-rbind(molten_pg_tree_b, molten_pg_tree_s) ## COMBINE THESE TWO DATA FRAMES BEACONS AND SCRIPTS TO A SINGLE DATA FRAME 

total_data<-arrange(total_data, id) ## Sort the data frame by ids so all beacons and scripts for each pagetree are in order

total_data
