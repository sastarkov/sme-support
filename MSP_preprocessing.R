if (!require("tictoc")) {
  install.packages("tictoc")
  library(tictoc)
}

if (!require("arrow")) {
  install.packages("arrow")
  library(arrow)
}

if (!require("dplyr")) {
  install.packages("dplyr")
  library(dplyr)
}

#Функция поиска дупликатов по inn в пределах месячных данных
find_dupl <- function(path_to_data) {

# Открываем датасет
  ds <- open_dataset(path_to_data)

  duplicates <- ds %>%
    add_count(month, inn, name = "cnt") %>%  #добавляет колонку cnt = количество строк year&месяц&inn 
    filter(cnt > 1) %>%  #выбираем те строки, где их количество больше 1
    select(-cnt) %>%  #удаляет колонку cnt
    collect()
  
  return(duplicates)
}

tic('Время выполнения:')
  dupl <- find_dupl("MSP_parsed/year=2017")
  print(dupl)
toc()