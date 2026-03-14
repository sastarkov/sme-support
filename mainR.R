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

if (!require("glue")) {
  install.packages("glue")
  library(glue)
}

if (!require("here")) {
  install.packages("here")
  library(here)
}

source(here("R", "RFSD_preprocessing.R")) # подключает файл с функциями для предобработки исходных данных из РББО
source(here("R", "MSP_preprocessing.R")) # подключает файл с функциями для предобработки исходных спарсенных данных из реестра МСП

tic('Время выполнения:')

# preprocess(2017:2024) # отбирает данные из РББО
find_dupl(here("RFSD_preprocessed"), 2017:2024) # проверяет наличие дубликатов записей (ИНН&год&месяц) в БД со спарсенными данными из реестра МСП

toc()