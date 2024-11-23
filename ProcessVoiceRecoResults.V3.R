###############################################################################
#Institute of Production Management
#University of Economics and Business, Vienna
#Course: Business Analytics, Alexander Prosser
#To be used for educational purposes only.
###############################################################################

#Load via Tools => Install packages
library(languageserver)
library(plyr)
library(readr)
library(stringr)
library(stringi)
library(tm)
library(sentimentr)
library(parallel)

###############################################################################

#READ FILES

###############################################################################

# Set directory of Complaints files


#read all *.csv in the above directory, create file list

#read all *.csv in the above directory, create file list
file_list = list.files(path="CallData", pattern="*.csv", full.names=TRUE)
file_list

# merge all filies into one file --> First apply read.csv, then rbind
myfiles = do.call(rbind, lapply(file_list, function(x) read.csv(x,  header=TRUE, sep=";",stringsAsFactors = FALSE)))

# read category file
category_DB <- read.csv2("cat/categories.csv", header = TRUE)
# Normally you would have thesaurus with synonyma.  

###############################################################################

#SPLIT SENTENCES and, or , but ,,

###############################################################################

#Create empty dataframe for result
clean_data= data.frame(Kundennummer=character(),
                       Datum=as.Date(character()),
                       CASE_ID=character(),
                       CONTENT=character(),
                       stringsAsFactors=FALSE)
#loop through rows of file with comments (Base is not 0 not 1)
#Read content row-wise and ...
for(i in 1:nrow(myfiles)) {
  
  v_KDNR=myfiles$Kundennummer[i]  #read Kundennummer
  v_DATUM=myfiles$Datum[i]        #read date
  v_CASE_ID=myfiles$CASE_ID[i]    #read CASE_ID
  v_CONTENT=myfiles$CONTENT[i]    #READ CONTENT

  #... split Content into sentences => Edit with students
  v_CONTENT_SPLIT=strsplit(v_CONTENT, split="whether | while | which | who | whom |However | however | and | or | but |,|\\.|\\!|\\?")
  
  #Create temporary results dataframe
  df_Lenght_split_result=data.frame(Reduce(rbind, v_CONTENT_SPLIT))
  
  #set column name
  names(df_Lenght_split_result)[1]<-"CONTENT"    #Column 1 Name
  df_Lenght_split_result$Datum=v_DATUM           #Append date
  df_Lenght_split_result$Kundennummer=v_KDNR     #Append Kundennummer
  df_Lenght_split_result$CASE_ID=v_CASE_ID       #Append CASE_ID
  #reorder by column index
  df_Lenght_split_result <- df_Lenght_split_result[c(3,4,2,1)]
  clean_data=rbind(clean_data,df_Lenght_split_result)  #Append temporary result table to result table
}

###############################################################################

#CLEAN DATA

###############################################################################

# clear all multiple blanks und trims
clean_data$CONTENT_CLEAN<-gsub("(?<=[//s])//s*|^//s+|//s+$", "", clean_data$CONTENT, perl=TRUE)           

# handling Negations don't --> do not
clean_data$CONTENT_CLEAN <- gsub("don't","do not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("doesn't","does not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("won't","will not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("can't","can not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("couldn't","could not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("didn't","did not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("shan't","shall not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("haven't","have not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("hasn't","has not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("isn't","is not", clean_data$CONTENT_CLEAN,ignore.case=T)
clean_data$CONTENT_CLEAN <- gsub("aren't","are not", clean_data$CONTENT_CLEAN,ignore.case=T)
#remove non aplphanumerical characters
clean_data$CONTENT_CLEAN<- gsub("[^a-zA-Z0-9 ]", "", clean_data$CONTENT_CLEAN)

#to lower
clean_data$CONTENT_CLEAN<-tolower(clean_data$CONTENT_CLEAN)

#stopwords have been used to parse content above, now they are removed as meaningless fillers. 
#To show list of stopwords, enter command line stopwords()
#add words to list
myStopwords <- c(stopwords('english'), "hm", "available", "via", "however", "naturally", "hi", "hello", "aha", "ok", "yes", "no", "oh", "hmm", "bye", "goodbye")
#myStopwords <- c("hm", "available", "via", "however", "naturally", "hi", "hello", "aha", "ok", "yes", "no", "oh", "hmm", "bye", "goodbye")
#delete words from list
Stopwords2Remove<-c("very", "not", "no", "am", "is", "are", "did", "i", "you", "he", "she", "we", "they")
myStopwords = myStopwords[!(myStopwords %in% Stopwords2Remove)]

# Remove stopwords either based on standard English stopword list or "private" stopword list
clean_data$CONTENT_CLEAN <- removeWords(clean_data$CONTENT_CLEAN, myStopwords)

#remove empty rows
#clean_data$CONTENT_CLEAN<-str_trim(clean_data$CONTENT_CLEAN, side = c("both", "left", "right"))
clean_data$CONTENT_CLEAN<-trimws(clean_data$CONTENT_CLEAN)
clean_data<-subset(clean_data, stri_length(clean_data$CONTENT_CLEAN)>0)

###############################################################################

#CLASSIFICATION according to categories.csv

###############################################################################

#Create empty dataframe for result
comments_categories <- data.frame(
  Kundennummer=character(),
  CASE_ID=character(),
  Datum=as.Date(character()),
  CONTENT_CLEAN=character(),
  display=character(),
  storagesize=character(),
  service=character(),
  coverage=character(),
  price=character(),
  stringsAsFactors=FALSE)
#Adapt to own categories

#Loop through result file clean_data (hint: nrow = number of rows)

# Stem document
clean_data$CONTENT_CLEAN = stemDocument(clean_data$CONTENT_CLEAN)

## Nested loop
#Split Text and find categories
for (i in 1:nrow(clean_data)){
  #print(paste(i, clean_data$CONTENT_CLEAN[i]))
  #results data frame
  v_KDNR=clean_data$Kundennummer[i]                       #get Kundennummer
  df_com_split<-data.frame(v_KDNR)                        #convert to dataframe
  colnames(df_com_split)[1]<-"Kundennummer"               #set column name
  df_com_split$CASE_ID=clean_data$CASE_ID[i]              #Append CASE_ID
  df_com_split$Datum=clean_data$Datum[i]                  #Append DATE
  df_com_split$CONTENT_CLEAN=clean_data$CONTENT_CLEAN[i]  #Append Content_Clean
  df_com_split$display=0                                  #Append Category 1 (topic 1) column
  df_com_split$storagesize=0                              #Append Category 2 (topic 2) column
  df_com_split$service=0                                 #Append Category 3 (topic 3) column
  df_com_split$coverage=0                                 #Append Category 4 (topic 4) column
  df_com_split$price=0                                    #Append Category 5 (topic 5) column 
  # For your own categories, change/add/delete above according to categories file
  
  #new category
  #df_com_split$new_category_name=0                                    #Append Category 5 (topic 5) column   
  
#create corpus
  corpus <- Corpus(VectorSource(clean_data$CONTENT_CLEAN[i]))   #Build Corpus
  td.mat <- as.matrix(TermDocumentMatrix(corpus))               #Convert Corpus to TDM
  df_td_mat<-data.frame(td.mat)                                 #Convert to dataframe
  
  if (nrow(df_td_mat)>0){
    
    for (x in 1:nrow(df_td_mat)){                                       #loop over rows of df_td_mat
    
      for (y in 1:nrow(category_DB)){                                   #loop over rows of Category_DB
        
        if (rownames(df_td_mat)[x] == category_DB$WORD[y]){             # if match
        
          gg<-unname(category_DB$CATEGORY[y])                           #get category
          #check for category, if match increase count by 1
      
          if (gg == "display"){ df_com_split$display[1]<-df_com_split$display[1]+1}
          
          if (gg == "storagesize"){ df_com_split$storagesize[1]<-df_com_split$storagesize[1]+1}
          
          if (gg == "service"){ df_com_split$service[1]<-df_com_split$service[1]+1}
          
          if (gg == "coverage"){ df_com_split$coverage[1]<-df_com_split$coverage[1]+1}
          
          if (gg == "price"){ df_com_split$price[1]<-df_com_split$price[1]+1}
          
          #new category => or delete/edit existing categories
          #if (gg == "new_category_name"){ df_com_split$new_category_name[1]<-df_com_split$new_category_name[1]+1}                              
          
        }
      }
    }
    comments_categories=rbind(comments_categories,df_com_split)  #Append temporary result table to result table
  }
}

## lapply
comments_categories <- mclapply(1:nrow(clean_data), function(i) {
  # Initialize the result dataframe for each row
  v_KDNR <- clean_data$Kundennummer[i]  # Get Kundennummer
  df_com_split <- data.frame(v_KDNR)    # Convert to dataframe
  colnames(df_com_split)[1] <- "Kundennummer"  # Set column name
  
  # Append necessary columns
  df_com_split$CASE_ID <- clean_data$CASE_ID[i]
  df_com_split$Datum <- clean_data$Datum[i]
  df_com_split$CONTENT_CLEAN <- clean_data$CONTENT_CLEAN[i]
  df_com_split$display <- 0
  df_com_split$storagesize <- 0
  df_com_split$service <- 0
  df_com_split$coverage <- 0
  df_com_split$price <- 0
  
  # Create corpus
  corpus <- Corpus(VectorSource(clean_data$CONTENT_CLEAN[i]))   # Build Corpus
  td.mat <- as.matrix(TermDocumentMatrix(corpus))               # Convert Corpus to TDM
  df_td_mat <- data.frame(td.mat)                               # Convert to dataframe
  
  # If the term-document matrix is not empty, proceed
  if (nrow(df_td_mat) > 0) {
    # Loop over rows of the term-document matrix (using lapply for rows)
    df_td_mat_rows <- mclapply(1:nrow(df_td_mat), function(x) {
      # Loop over rows of category_DB (using lapply for rows)
      category_match <- mclapply(1:nrow(category_DB), function(y) {
        if (rownames(df_td_mat)[x] == category_DB$WORD[y]) {
          gg <- unname(category_DB$CATEGORY[y])  # Get the category
          # Check for category and increment the corresponding column in df_com_split
          if (gg == "display") {
            df_com_split$display <- df_com_split$display + 1
          } else if (gg == "storagesize") {
            df_com_split$storagesize <- df_com_split$storagesize + 1
          } else if (gg == "service") {
            df_com_split$service <- df_com_split$service + 1
          } else if (gg == "coverage") {
            df_com_split$coverage <- df_com_split$coverage + 1
          } else if (gg == "price") {
            df_com_split$price <- df_com_split$price + 1
          }
        }
      })
    })
  }
  
  return(df_com_split)  # Return the result for this row
})

# Convert to dataframe
comments_categories <- data.frame(t(sapply(comments_categories,c)))
###############################################################################

#SENTIMENT

###############################################################################

#Save data frame
comments_categories_sentiment<-comments_categories                 #move comments categories to new sentiment structure
comments_categories_sentiment$Sentiment<-0                         #add and intitialize Column for Sentiment Value

#loop through results file to assign sentiments
#for (i in 1:nrow(comments_categories_sentiment)){
#  v_sentiment=sentiment(comments_categories_sentiment$CONTENT_CLEAN[i])  #Calculate Sentiment
#  comments_categories_sentiment$Sentiment[i]=v_sentiment[1,4]            #write sentiment value to result file
#}

start_mc <- Sys.time()
# Create a cluster of workers
num_cores <- detectCores() - 1  # Detect the number of available cores and leave one core free
cl <- makeCluster(num_cores)    # Create a cluster with the specified number of cores

# Export necessary functions and data to the cluster
clusterExport(cl, list("sentiment", "comments_categories_sentiment")) 

# Use parLapply to apply the sentiment function in parallel across rows of the data frame
comments_categories_sentiment$Sentiment <- parLapply(cl, 1:nrow(comments_categories_sentiment), function(i) {
  
  # Extract the clean content text from the row
  clean_content <- as.character(comments_categories_sentiment$CONTENT_CLEAN[i])  # Ensure it's a character vector
  
  # Calculate sentiment for the clean content using sentimentr::sentiment()
  v_sentiment <- sentiment(clean_content)  # Calculate sentiment
  
  # Return the sentiment value (typically the first row of the output data frame)
  return(v_sentiment$sentiment[1])  # Assuming the result is a data frame with a 'sentiment' column
  
})

# Stop the cluster to free up resources
stopCluster(cl)
end_mc <- Sys.time()
duration_mc <- end_mc - start_mc
print(duration_mc)

# It took 9.75 seconds using 11 cores

start_sc <- Sys.time()
comments_categories_sentiment$Sentiment <- apply(comments_categories_sentiment, 1, function(row) {
  # Extract the clean content text from the row
  clean_content <- as.character(row["CONTENT_CLEAN"])  # Ensure it's a character vector
  
  # Calculate sentiment for the clean content using sentimentr::sentiment()
  v_sentiment <- sentiment(clean_content)  # Calculate sentiment
  
  # Return the sentiment value (typically the first row of the output data frame)
  return(v_sentiment$sentiment[1])  # Assuming the result is a data frame with a 'sentiment' column
})
end_sc <- Sys.time()
duration_sc <- end_sc - start_sc
print(duration_sc)

# It took 52 seconds using single core

#Prepare Write file: make sentiment score a string and suppress the text. 
comments_categories_sentiment$Sentiment<-as.numeric(comments_categories_sentiment$Sentiment)
comments_categories_sentiment_print<-comments_categories_sentiment
comments_categories_sentiment_print$CONTENT_CLEAN<-NULL

# Write data frame comments categories into file in file MyData.csv in Working Directory
write.table(comments_categories_sentiment_print, file = "ClassifiedData.csv", sep=";")

###############################################################################

#End => Continue in HANA

###############################################################################