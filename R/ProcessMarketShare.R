# Novo BDI extraido
my.folder<-'//Zeus//monitoramento_compliance_bi$//R//Projects//BDI//'
setwd(my.folder)
rm(my.folder)
getwd()

# Library ---------------------------------------------------------------
library(GetHFData)
library(reshape)
library(tidyr)
library(dplyr)


# Functions ---------------------------------------------------------------

write.excel <- function(x,row.names=FALSE,col.names=TRUE,...) {
  write.table(x,"clipboard",sep="\t",row.names=row.names,col.names=col.names,...)
}

# TradeDate Vector ------------------------------------------------------
load("//zeus/monitoramento_compliance_bi$/R/Projects/BDI/Players.RData")
df.date<-data.frame(as.character(df.players$SessionDate))
df.date<-distinct(df.date)
colnames(df.date)<-c('SessionDate')
date<-as.character(last(df.date$SessionDate))

equity.files      <- ghfd_get_ftp_contents(type.market = 'equity')
equity_odds.files <- ghfd_get_ftp_contents(type.market = 'equity-odds')
options.files     <- ghfd_get_ftp_contents(type.market = 'options')
bmf.files         <- ghfd_get_ftp_contents(type.market = 'BMF')

equity.files      <- distinct(equity.files)
equity_odds.files <- distinct(equity_odds.files)
options.files     <- distinct(options.files)
bmf.files         <- distinct(bmf.files)

equity.files$Market      <- 'equity'
equity_odds.files$Market <- 'equity-odds'
options.files$Market     <- 'options'
bmf.files$Market         <- 'bmf'


df.list<-list(equity.files,equity_odds.files,options.files,bmf.files)

x<-merge_recurse(df.list)
x<-distinct(x)
x<-x[x$dates >date,]

rm(equity.files,options.files,equity_odds.files,bmf.files,df.list,df.date)


# Download DataSets from ftp.bmf.com.br -----------------------------------
i=1

Download.Inicio<-Sys.time()

repeat{
  print(i)
  #cat("\014") # clean Consolse
  my.ftp <- as.character(x[i, 3])
  out.file <- paste0("./ftp files/",as.character(x[i, 1]))
  ghfd_download_file(my.ftp = my.ftp, out.file = out.file)
  
  i = i + 1
  gc()
  
  if (i > length(x[,1])) {
    
    break
  }
}

Download.Fim<-Sys.time()

# Read DataFiles  ---------------------------------------------------------

my.assets     <- NULL       #  Select all Assets in the file
type.matching <- 'exact'    # defines how to match assets in dataset
start.time    <- '09:00:00' # defines first time period of day
end.time      <- '19:00:00' # defines last time period of day
agg.diff      <- '10 hours'


# #Creating the DataFrame -------------------------------------------------------
raw.start.time=Sys.time()

#Create File List DF
list.files<-x
list.files$link<-NULL
list.files<-spread(list.files,Market,files)
list.files<-list.files[which(!is.na(list.files$bmf) & !is.na(list.files$equity) & 
                             !is.na(list.files$`equity-odds`) & !is.na(list.files$options)),]

rownames(list.files)<-1:nrow(list.files)

i=1

repeat{
  
  date<- list.files[i,1] #Get date of i variable
  print(paste0(i, " of ",length(list.files[,1]), " Records ",date))
  ##Raw Data
  type.output   <- 'raw'
  #Equity
  type.market <- 'equity'
  out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 3 ]) #Concatenate Path&FileName of i
  
  raw.equity.tmp <- ghfd_read_file(out.file, 
                                   type.matching = type.matching,
                                   my.assets   = my.assets,
                                   first.time  = start.time,
                                   last.time   = end.time,
                                   type.output = type.output,
                                   agg.diff    = agg.diff)
  
  #Equity-Odds
  type.market <- 'equity-odds'
  out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 4 ]) #Concatenate Path&FileName of i
  
  raw.equity.odds.tmp <- ghfd_read_file(out.file, 
                                        type.matching = type.matching,
                                        my.assets   = my.assets,
                                        first.time  = start.time,
                                        last.time   = end.time,
                                        type.output = type.output,
                                        agg.diff    = agg.diff)
  
  #Options
  type.market <- 'options'
  out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 5 ]) #Concatenate Path&FileName of i
  
  raw.options.tmp <- ghfd_read_file(out.file, 
                                    type.matching = type.matching,
                                    my.assets   = my.assets,
                                    first.time  = start.time,
                                    last.time   = end.time,
                                    type.output = type.output,
                                    agg.diff    = agg.diff)
  
  #BMF
  type.market <- 'bmf'
  out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 2 ]) #Concatenate Path&FileName of i
  
  raw.bmf.tmp <- ghfd_read_file(out.file, 
                                type.matching = type.matching,
                                my.assets   = my.assets,
                                first.time  = start.time,
                                last.time   = end.time,
                                type.output = type.output,
                                agg.diff    = agg.diff)
  
  gc()
  
  if (length(raw.equity.tmp) != 0  ) { 
    
    raw.bmf.tmp$Market<-'BMF'
    raw.equity.odds.tmp$Market<-'Equity-Odds'
    raw.equity.tmp$Market<-'Equity'
    raw.options.tmp$Market<-'Options'
    
    list.raw<-list(raw.bmf.tmp,raw.equity.odds.tmp,raw.equity.tmp,raw.options.tmp)
    
    raw.df.tmp<-merge_recurse(list.raw)
    
    rm(raw.bmf.tmp,raw.equity.odds.tmp,raw.equity.tmp,raw.options.tmp,list.raw)
    
    raw.df <- raw.df.tmp %>%
      group_by(Market,
               SessionDate,
               CrossTradeIndicator,
               BuyMember,
               SellMember,
               TradeSign) %>%
      summarise(
        n_Trades        = n(),
        Traded_Qty      = sum(TradedQuantity),
        Traded_Vol      = sum(TradePrice * TradedQuantity)
        
      ) %>%
      arrange(SessionDate)
    
    rm(raw.df.tmp)
    gc()
    if(!exists("df.players")) {
      df.raw<- raw.df %>% mutate_if(is.numeric, round, digits=2)
      df.players<-df.raw
    } else {
      df.raw<- raw.df %>% mutate_if(is.numeric, round, digits=2)
      df.players<-rbind(df.players,df.raw)
    }
    
  } else { print(date)}
  
  i = i + 1
  if (i > length(list.files[,1])  ) { 
    break
  }
}

gc()
raw.end.time=Sys.time()

save(df.players,file = "//zeus/monitoramento_compliance_bi$/R/Projects/BDI/Players.RData")

write.csv2(df.players,file = paste0('./Output Files/',"BDI-FullPlayers.csv"))
