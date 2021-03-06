
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

ghfd_get_ftp_contents <- function(type.market = 'equity',
                                  max.dl.tries = 10,
                                  type.data = 'trades'){
  
  # check type.market
  possible.names <- c('equity','equity-odds','options','BMF')
  idx <- type.market %in% possible.names
  
  if (!any(idx)){
    stop(paste(c('Input type.market not valid. It should be one of the following: ', possible.names), collapse = ', '))
  }
  
  # check type.data
  possible.names <- c('trades','orders')
  idx <- type.data %in% possible.names
  
  if (!any(idx)){
    stop(paste(c('Input type.data not valid. It should be one of the following: ', possible.names), collapse = ', '))
  }
  
  # test for internet
  test.internet <- curl::has_internet()
  
  if (!test.internet){
    stop('No internet connection found...')
  }
  
  # set ftp site
  if (type.market == 'equity')      my.ftp <- "ftp://ftp.bmf.com.br/marketdata/Bovespa-Vista/"
  if (type.market == 'equity-odds') my.ftp <- "ftp://ftp.bmf.com.br/marketdata/Bovespa-Vista/"
  if (type.market == 'options') my.ftp <- "ftp://ftp.bmf.com.br/MarketData/Bovespa-Opcoes/"
  if (type.market == 'BMF')     my.ftp <- "ftp://ftp.bmf.com.br/marketdata/BMF/"
  
  # set time stop (ftp seems to give wrong files sometimes..)
  Sys.sleep(1)
  
  i.try <- 1
  while (TRUE){
    cat(paste('\nReading ftp contents for ',type.market, '(',type.data,')',
              ' (attempt = ', i.try,'|',max.dl.tries,')',sep = ''))
    files.at.ftp <- NULL
    try({
      files.at.ftp <- RCurl::getURL(my.ftp,
                                    verbose=F,
                                    ftp.use.epsv=FALSE,
                                    dirlistonly = TRUE)
    })
    
    if (type.data =='trades'){
      # filter ftp files for trades
      pattern.files <- 'NEG_(.*?).zip'
    } else if (type.data == 'orders') {
      pattern.files <- 'OFER_(.*?).zip'
    }
    
    files.at.ftp <- stringr::str_extract_all(files.at.ftp,
                                             pattern = pattern.files )[[1]]
    
    # remove or not FRAC market files
    idx <- stringr::str_detect(files.at.ftp, pattern = stringr::fixed('FRAC'))
    
    if (type.market=='equity-odds'){
      files.at.ftp <- files.at.ftp[idx]
    } else {
      
      files.at.ftp <- files.at.ftp[!idx]
    }
    
    # remove BMF files in Bovespa equity (why are these files there??)
    
    if ((type.market =='equity')|(type.market=='equity-odds')){
      idx <- stringr::str_detect(files.at.ftp, pattern = stringr::fixed('BMF'))
      files.at.ftp <- files.at.ftp[!idx]
      
      idx <- stringr::str_detect(files.at.ftp, pattern = stringr::fixed('OPCOES'))
      files.at.ftp <- files.at.ftp[!idx]
    }
    
    # remove larger zip files with several txt files (only a couple of months)
    # DEPRECATED: THESE FILES WITH LARGE NAMES ARE NO LONGER IN THE FTP
    #idx <- sapply(files.at.ftp, FUN = function(x) return(stringr::str_count(x,pattern = '_')))<3
    #files.at.ftp <- files.at.ftp[idx]
    
    # check if html.code and size makes sense. If not, download it again
    
    if ( is.null(files.at.ftp)|(length(files.at.ftp)<200) ){
      cat(' - Error in reading ftp contents. Trying again..')
    } else {
      break()
    }
    
    if (i.try==max.dl.tries){
      stop('Reached maximum number of attempts to read ftp content. Exiting now...')
    }
    
    i.try <- i.try + 1
    
    Sys.sleep(2)
  }
  
  # find dates from file names
  
  ftp.dates <- unlist(stringr::str_extract_all(files.at.ftp,
                                               pattern = paste0(rep('[0-9]',8),
                                                                collapse = '')))
  ftp.dates <- as.Date(ftp.dates,format = '%Y%m%d')
  
  
  
  df.ftp <- data.frame(files = as.character(files.at.ftp),
                       dates = ftp.dates,
                       link = as.character(paste0(my.ftp,as.character(files.at.ftp))))
  
  if (type.data == 'orders') {
    df.ftp$type.order <- ifelse(stringr::str_detect(files.at.ftp,
                                                    stringr::fixed('VDA')), 'Sell','Buy')
  }
  
  return(df.ftp)
  
}
# TradeDate Vector ------------------------------------------------------
load("//zeus/monitoramento_compliance_bi$/R/Projects/BDI/BDI.RData")
df.date<-data.frame(as.character(df.BDI.final$SessionDate))
df.date<-distinct(df.date)
colnames(df.date)<-c('SessionDate')
date<-as.character(head(df.date$SessionDate,1))

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
    
    raw.df<- raw.df.tmp %>%
      group_by(
        Market,
        SessionDate, 
        InstrumentSymbol
      ) %>%
      summarise(
        open.price      = first(TradePrice),  
        min.price       = min(TradePrice),
        max.price       = max(TradePrice),
        avg.price       = round(mean(TradePrice), digits = 2)
        ) %>%
      arrange(
        SessionDate, 
        InstrumentSymbol
      )
    
    rm(raw.df.tmp)
    gc()
    df.raw<- raw.df %>% mutate_if(is.numeric, round, digits=2)
    rm(raw.df)
    
    
    ## Agg Data
    type.output   <- 'agg'
    #Equity
    type.market <- 'equity'
    out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 3 ]) #Concatenate Path&FileName of i
    
    agg.equity.tmp <- ghfd_read_file(out.file, 
                                     type.matching = type.matching,
                                     my.assets   = my.assets,
                                     first.time  = start.time,
                                     last.time   = end.time,
                                     type.output = type.output,
                                     agg.diff    = agg.diff)
    
    #Equity-Odds
    type.market <- 'equity-odds'
    out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 4 ]) #Concatenate Path&FileName of i
    
    agg.equity.odds.tmp <- ghfd_read_file(out.file, 
                                          type.matching = type.matching,
                                          my.assets   = my.assets,
                                          first.time  = start.time,
                                          last.time   = end.time,
                                          type.output = type.output,
                                          agg.diff    = agg.diff)
    
    #Options
    type.market <- 'options'
    out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 5 ]) #Concatenate Path&FileName of i
    
    agg.options.tmp <- ghfd_read_file(out.file, 
                                      type.matching = type.matching,
                                      my.assets   = my.assets,
                                      first.time  = start.time,
                                      last.time   = end.time,
                                      type.output = type.output,
                                      agg.diff    = agg.diff)
    
    #BMF
    type.market <- 'bmf'
    out.file = paste0('./ftp files/',list.files[ which(list.files$dates == date) , 2 ]) #Concatenate Path&FileName of i
    
    agg.bmf.tmp <- ghfd_read_file(out.file, 
                                  type.matching = type.matching,
                                  my.assets   = my.assets,
                                  first.time  = start.time,
                                  last.time   = end.time,
                                  type.output = type.output,
                                  agg.diff    = agg.diff)
    gc()
    
    agg.bmf.tmp$Market<-'BMF'
    agg.equity.odds.tmp$Market<-'Equity-Odds'
    agg.equity.tmp$Market<-'Equity'
    agg.options.tmp$Market<-'Options'
    
    list.agg<-list(agg.bmf.tmp,agg.equity.odds.tmp,agg.equity.tmp,agg.options.tmp)
    
    agg.df.tmp<-merge_recurse(list.agg)
    
    rm(agg.bmf.tmp,agg.equity.odds.tmp,agg.equity.tmp,agg.options.tmp,list.agg)
    
    df.agg<-agg.df.tmp
    rm(agg.df.tmp)
    
    ##Standarize Fields
    df.agg$weighted.price<-round(df.agg$weighted.price, digits = 2)
    df.agg$period.ret<-round(df.agg$period.ret*100, digits = 2)
    df.agg$period.ret.volat<-round(df.agg$period.ret.volat*100, digits = 2)
    
    BDI.tmp<-left_join(df.raw,df.agg, by = c("Market","SessionDate","InstrumentSymbol"))
    BDI.tmp$TradeDateTime<-NULL
    BDI.tmp$Tradetime<-NULL
    df.BDI<-BDI.tmp
    df.BDI<-data.frame(df.BDI)
    
    df.BDI<-do.call(data.frame,lapply(df.BDI, function(x) replace(x, is.infinite(x),NA)))
    df.BDI[is.na(df.BDI)]<-0
    df.BDI<-df.BDI[c('Market','SessionDate','InstrumentSymbol','open.price','min.price',
                     'max.price','avg.price','last.price','n.trades','n.buys','n.sells',
                     'sum.qtd','sum.vol','weighted.price','period.ret','period.ret.volat')]
    
    write.csv2(df.BDI,file = paste0('./Output Files/',date,"-BDI.csv"))
    rm(BDI.tmp,df.agg,df.BDI,df.raw)
    gc()
  } else { print(date)}
  
  i = i + 1
  if (i > length(list.files[,1])  ) { 
    break
  }
}

gc()
raw.end.time=Sys.time()


# # Collect BDI Data ------------------------------------------------------
load("//zeus/monitoramento_compliance_bi$/R/Projects/BDI/BDI.RData")
df.BDI<-df.BDI.final[,1:17]

i=1

repeat{
  print(i)
  date<- list.files[i,1] #Get date of i variable
  #cat("\014") # clean Consolse
  df.BDI.tmp <- read.csv2(file = paste0('./Output Files/',date,"-BDI.csv"), header = TRUE)
  if(!exists("df.BDI")) {
    df.BDI<-df.BDI.tmp
  } else {
    df.BDI<-rbind(df.BDI.tmp,df.BDI)
  }
  i = i + 1
  
  gc()
  
  if (i > length(list.files[,1])) { 
    
    break
  }
}

df.BDI.Dates<-data.frame(df.BDI[,'SessionDate'])
colnames(df.BDI.Dates)<-c('SessionDate')
df.BDI.Dates<-distinct(df.BDI.Dates)
df.BDI.Dates<-arrange(df.BDI.Dates,desc(SessionDate))

df.BDI$TradeDate_ID<-NULL
df.BDI$TradeDate_ID_B<-NULL

df.BDI.Dates$TradeDate_ID<-1:nrow(df.BDI.Dates)
df.BDI.Dates$TradeDate_ID_B<-df.BDI.Dates$TradeDate_ID-1

colnames(df.BDI.Dates)<-c('SessionDate','TradeDate_ID','TradeDate_ID_B')

df.BDI.final<-left_join(df.BDI,df.BDI.Dates, by='SessionDate')
df.BDI.final<-df.BDI.final %>% mutate_if(is.numeric, round, digits=2)

df.BDI.lastDate<-df.BDI.final[,c('InstrumentSymbol','TradeDate_ID','last.price')]
colnames(df.BDI.lastDate)<-c('InstrumentSymbol','TradeDate_ID_B','last.price.dayBefore')

df.BDI.final<-left_join(df.BDI.final,df.BDI.lastDate,by=c('InstrumentSymbol','TradeDate_ID_B'))
df.BDI.final$Price_Osc_D1<-(((df.BDI.final$last.price/df.BDI.final$last.price.dayBefore)-1)*100)
df.BDI.final<-df.BDI.final %>% mutate_if(is.numeric, round, digits=2)

rm(df.BDI,df.BDI.Dates,df.BDI.lastDate,df.BDI.tmp)

df.BDI.final<-distinct(df.BDI.final)

df.BDI.final<-do.call(data.frame,lapply(df.BDI.final, function(x) replace(x, is.infinite(x),NA)))

rm(list.files,x)
rm(agg.diff,date,Download.Fim,Download.Inicio,end.time,i,my.assets,
   my.ftp,out.file,raw.end.time,raw.start.time,start.time,type.market,
   type.matching,type.output,write.excel)

save(df.BDI.final,file = "//zeus/monitoramento_compliance_bi$/R/Projects/BDI/BDI.RData")

write.csv2(df.BDI.final,file = paste0('./Output Files/',"BDI-Full.csv"))
