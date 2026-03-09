# проверка наличия пакетов, установка при необходимости и загрузка
if (!require("arrow")) {
  install.packages("arrow")
  library(arrow)
}

if (!require("dplyr")) {
  install.packages("dplyr")
  library(dplyr)
}

if (!require("glue")) {
  install.packages("glue")
  library(glue)
}

# Функция для извлечения нужных признаков в заданный год и предобработки данных
extract_data <- function(demand_year, demand_month) {
  
  ds <- open_dataset(
    "MSP_parsed",
    partitioning = schema(year = string(), month = string())
  ) # путь к папке с БД .parquet
  
     df <- ds %>% 
     filter(year == demand_year, month == demand_month) %>% 
     collect()

  return(df)
}


df1 <- extract_data('2017', '01')
df2 <- extract_data('2017', '02')

df <- full_join(df1, df2, by = "inn", suffix = c("_jan", "_feb"))