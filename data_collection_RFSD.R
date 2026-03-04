# проверка наличия пакетов, установка при необходимости и загрузка
if (!require("arrow")) {
  install.packages("arrow")
  library(arrow)
}

if (!require("dplyr")) {
  install.packages("dplyr")
  library(dplyr)
}

# # Откройте датасет как "Dataset" (ленивая загрузка)
# ds <- open_dataset("RFSD")

# # Посмотрите на схему данных (имена колонок и их типы)
# schema <- ds$schema
# print(schema)

# # Загрузите небольшую выборку, чтобы понять содержимое
# sample_df <- ds |> filter(year == 2024) |> head(10) |> collect() 
# glimpse(sample_df)
# names(ds)

# # n_2017 <- ds |> filter(year == 2017) |> count() |> collect() |> pull(n)
# # n_2017

# Функция для извлечения нужных признаков в заданный год
extract_data <- function(required_year) {
  
  ds <- open_dataset("RFSD") # путь к папке с БД .parquet
  
  # Список базовых дескрипторов фирмы и данных о ней
  descr <- c(
    "year", "inn", "ogrn", "region", "creation_date", "dissolution_date",
    "age", "financial", "okved_section", "okved", "okopf", "okogu",
    "okfc", "oktmo", "eligible", "filed", "exemption_criteria", "simplified",
    "imputed", "articulated", "totals_adjustment", "outlier"
  )
  
  # Список извлекаемых строк бухгалтерской отчетности
  lines <- c( 
    "line_1100", "line_1110", "line_1120", "line_1130", "line_1140", "line_1150",         
    "line_1160", "line_1170", "line_1180", "line_1190", "line_1200", "line_1210",
    "line_1220", "line_1230", "line_1240", "line_1250", "line_1260", "line_1300", 
    "line_1310", "line_1320", "line_1340", "line_1350", "line_1360", "line_1370",
    "line_1400", "line_1410", "line_1420", "line_1430", "line_1450", "line_1500",
    "line_1510", "line_1520", "line_1530", "line_1540", "line_1550", "line_1600",
    "line_1700", "line_2100", "line_2110", "line_2120", "line_2200", "line_2210",
    "line_2220", "line_2300", "line_2310", "line_2320", "line_2330", "line_2340",
    "line_2350", "line_2400"
  ) 
  
  signs_used <- c(descr, lines) # объединяем списки
    
  # Загружаем только нужные признаки в требуемый год
  df <- ds %>% 
    filter(year == required_year) %>%
    select(signs_used) %>%
    collect()

  return(df)
}

df_year <- extract_data(2017)
glimpse(df_year)