# Novo Investimento Pessoal
my.folder<-'//Zeus//monitoramento_compliance_bi$//R//Projects//InvestimentoPessoal//'
setwd(my.folder)
rm(my.folder)
getwd()

# Library ---------------------------------------------------------------
library(reshape)
library(tidyr)
library(dplyr)
library(readxl)
library(RDCOMClient)
library(data.table)
library(xlsx)

# Data Gathering ----------------------------------------------------------
df.files<-data.frame(list.files('\\\\Zeus\\supt_cpl_ci_bi_asset$\\TEMP_Investimentopessoal\\XLSX\\'))
colnames(df.files)<-'FileName'
df.files<-distinct(df.files)
df.xls<-subset(df.files,grepl("^.+xls+$",FileName))
df.xls<-subset(df.xls,grepl("(R-|R - |R- )",FileName))
write.csv2(df.xls,"//Zeus//monitoramento_compliance_bi$//R//Projects//InvestimentoPessoal//ArquvivoXLS.csv")
df.files<-subset(df.files,grepl("^.+xlsx+$",FileName))
df.files<-subset(df.files,grepl("(R-|R - |R- )",FileName))

path="\\\\Zeus\\supt_cpl_ci_bi_asset$\\TEMP_Investimentopessoal\\XLSX\\"


col_types <- c("date","date","date","text","text","text","text","text","text","text","text","text",
              "text","text","numeric","text","text")
i<-1

repeat{
  print(i)
  df.Invest.Pessoal_tmp<-data.frame( DataPrenchimento=character(),
                                     DataRecebimento=character(),
                                     DataRespostaCompliance=character(),NomeColaborador=character(),
                                     CPFColaborador=character(),AreaColaborador=character(),
                                     GestorArea=character(),NomeCompletoConjuge=character(),
                                     CPFConjuge= character(),DeclaracaoInvestidor=character(),
                                     NaturezaOperacao=character(),AtivoHaTrintaDias=character(),
                                     Ativo=character(),CodigoAtivo=character(),Quantidade=integer(),
                                     ParecerComplianceBI=character(),Observacoes=character())
  
  df.Invest.Pessoal_excel<-try(read_excel(path =  paste0(path , df.files[i,]) , 
                                      sheet=2, col_names = TRUE, col_types = col_types, skip = 0))
  
  
  
  if(class(df.Invest.Pessoal_excel)!="try-error"){
    df.Invest.Pessoal_excel<-try(df.Invest.Pessoal_excel[which(!is.na(df.Invest.Pessoal_excel$NomeColaborador)),])
    df.Invest.Pessoal_excel<-df.Invest.Pessoal_excel[which(!is.na(df.Invest.Pessoal_excel$NomeColaborador)),]
    df.Invest.Pessoal_tmp<-rbind(df.Invest.Pessoal_tmp,df.Invest.Pessoal_excel)
    df.Invest.Pessoal_tmp<-cbind(df.Invest.Pessoal_tmp,FileName=paste0(path , df.files[i,]))
  
    if(!exists("df.Invest.Pessoal")) {
      df.Invest.Pessoal<-df.Invest.Pessoal_tmp
    } else {
      df.Invest.Pessoal<-rbind(df.Invest.Pessoal,df.Invest.Pessoal_tmp)
      df.Invest.Pessoal<-distinct(df.Invest.Pessoal)
    }
  
  }
  
  i = i + 1
  gc()
  
  if (i > length(df.files[,1])) { 
    break
  }
}

write.csv2(df.Invest.Pessoal,"InvestPessoal.csv")
