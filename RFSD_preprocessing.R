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
extract_data <- function(required_year) {
  
  ds <- open_dataset("RFSD") # путь к папке с БД .parquet
  
  # Список базовых дескрипторов фирмы и данных о ней
  descr <- c(
    "year", "inn", "region", "creation_date", "dissolution_date",
    "age", "okved_section", "okved", "okopf", "okfc", "oktmo", 
    "filed", "imputed", "simplified", "articulated", "totals_adjustment", 
    "outlier"
  )
  
  # Список извлекаемых строк бухгалтерской отчетности
  lines <- c( 
    "line_1100", "line_1110", "line_1120", "line_1130", "line_1140", "line_1150",         
    "line_1160", "line_1170", "line_1180", "line_1190", "line_1200", "line_1210",
    "line_1220", "line_1230", "line_1240", "line_1250", "line_1260", "line_1300", 
    "line_1370", "line_1400", "line_1410", "line_1420", "line_1430", "line_1450", 
    "line_1500", "line_1510", "line_1520", "line_1530", "line_1540", "line_1550", 
    "line_1600", "line_1700", "line_2100", "line_2110", "line_2120", "line_2200", 
    "line_2210", "line_2220", "line_2300", "line_2330", "line_2340", "line_2350", 
    "line_2400"
  ) 
  
  signs_used <- c(descr, lines) # объединяем списки базовых дескрипторов и строк бухгалтерской отчетности
    
  # Загружаем только нужные признаки в требуемый год
  df <- ds %>% 
    filter(year == required_year) %>%
    filter(!is.na(inn)) %>%                   # отфильтровываем ошибочные записи без inn
    filter(eligible == 1) %>%                 # оставляем только фирмы, которые обязаны предоставлять отчетность
    filter(financial == 0) %>%                # оставляем не финансовые фирмы 
    filter(exemption_criteria == 'none') %>%  # оставляем не финансовые, не религиозные, не государственные и не новые (IV квартал создания) организации 
    select(all_of(signs_used)) %>%
    collect()

  return(df)
}

# Функция, записывающая новую БД .parquet с предочищенными и отбранными данными
preprocess <- function(year_list) {

  for(year_one in year_list) {
  
    df_year <- extract_data(year_one)
    write_dataset(df_year, 
                path = "RFSD_preprocessed",
                partitioning = "year",
                format = "parquet",
                existing_data_behavior = "delete_matching") # корректная перезапись существующих данных
    
    cat(glue("Данные РББО за {year_one} год успешно предобработаны и сохранены."))
  }
}

preprocess(2017:2024)

