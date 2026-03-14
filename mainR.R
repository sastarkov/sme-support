if (!require("tictoc")) {
  install.packages("tictoc")
  library(tictoc)
}

if (!require("here")) {
  install.packages("here")
  library(here)
}

source(here("R_scripts", "RFSD_preprocessing.R")) # подключает файл с функциями для предобработки исходных данных из РББО
source(here("R_scripts", "MSP_preprocessing.R")) # подключает файл с функциями для предобработки исходных спарсенных данных из реестра МСП

tic('Время выполнения:')

# preprocess(2017:2024) # отбирает данные из РББО
find_dupl(here("Data/MSP_parsed"), 2026) # проверяет наличие дубликатов записей (ИНН&год&месяц) в БД со спарсенными данными из реестра МСП
# clean_dupl('Data/MSP_parsed', 2026)

toc()