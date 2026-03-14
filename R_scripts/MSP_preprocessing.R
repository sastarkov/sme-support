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

#Функция поиска дупликатов по inn в пределах месячных данных
# find_dupl <- function(path_to_data) {

# # Открываем датасет
#   ds <- open_dataset(path_to_data)

#   duplicates <- ds %>%
#     add_count(month, inn, name = "cnt") %>%  #добавляет колонку cnt = количество строк year&месяц&inn 
#     filter(cnt > 1) %>%  #выбираем те строки, где их количество больше 1
#     select(-cnt) %>%  #удаляет колонку cnt
#     collect()
  
#   return(duplicates)
# }

# Функция для поиска дубликатов записей в спарсенном датасете МСП
find_dupl <- function(dir_data, year_list) {

  all_dupl <- tibble()

  for(year_one in year_list) {
    
    ds <- open_dataset(glue("{dir_data}/year={year_one}"))

    dupl <- ds %>%
    add_count(month, inn, name = "cnt") %>%  #добавляет колонку cnt = количество строк year&месяц&inn 
    filter(cnt > 1) %>%  #выбираем те строки, где их количество больше 1
    select(-cnt) %>%  #удаляет колонку cnt
    collect()
        
    n_dupl_inn <- nrow(dupl)
    unique_inn <- n_distinct(dupl$inn)

    print(glue('В данных за {year_one} год {n_dupl_inn} дублирующихся строк, среди них уникальных ИНН - {unique_inn}.'))

    all_dupl <- bind_rows(all_dupl, dupl)

  }
  return(all_dupl)
}

# Функция для удаления дубликатов записей в спарсенном датасете МСП в конкретный год
clean_dupl <- function(dir_data, year_one) {

  ds <- open_dataset(glue("{dir_data}/year={year_one}"))

  # Дедупликация
  clean_data <- ds %>%
    distinct(month, inn, .keep_all = TRUE)

  # Запись в НОВУЮ папку
  write_dataset(
    clean_data,
    glue("{dir_data}_cleaned/year={year_one}"),  # Новый путь
    format = "parquet",
    partitioning = c("month")  # Сохраняем структуру партиций
  )
  print(glue("Данные за {year_one} год очищены и помещены в папку {{dir_data}_cleaned}"))
  }





