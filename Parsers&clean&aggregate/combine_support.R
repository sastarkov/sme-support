# Необходимые пакеты
library(arrow)
library(dplyr)
library(stringr)
library(purrr)

combine_parquet_sequential <- function(path, pattern = "\\.parquet$") {
  # Список всех parquet-файлов в папке
  files <- list.files(path, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) stop("В указанной папке нет файлов .parquet")
  
  # Извлекаем ГГГГММ из имени файла
  file_info <- tibble(
    file = files,
    basename = basename(files),
    date_str = str_extract(basename, "\\d{6}")
  ) %>% 
    filter(!is.na(date_str)) %>%        # оставляем только файлы с датой
    arrange(date_str)                   # сортируем от ранних к поздним
  
  if (nrow(file_info) == 0) stop("Нет файлов с шаблоном ГГГГММ в имени")
  
  # Читаем самый ранний файл
  message("Читаю начальный файл: ", file_info$basename[1])
  full_df <- read_parquet(file_info$file[1]) %>% select(inn, inn_prov, date_decision_support, date_supp, support_form, support_type, support_amount, support_unit)
  
  if (nrow(file_info) == 1) {
    message("Найден только один файл, возвращаю его.")
    return(full_df)
  }
  
  # Последовательно обрабатываем остальные файлы
  for (i in 2:nrow(file_info)) {
    message("Обрабатываю: ", file_info$basename[i])
    next_df <- read_parquet(file_info$file[i])  %>% select(inn, inn_prov, date_decision_support, date_supp, support_form, support_type, support_amount, support_unit)
    
    # Столбцы, общие для обоих датафреймов
    common_cols <- intersect(names(next_df), names(full_df))
    
    if (length(common_cols) == 0) {
      warning("Нет общих столбцов, добавляю все строки без дедупликации.")
      new_rows <- next_df
    } else {
      # Строки из next_df, которых ещё нет в full_df
      new_rows <- anti_join(next_df, full_df, by = common_cols)
    }
    
    full_df <- bind_rows(full_df, new_rows)
    message(sprintf("  → добавлено %d новых строк", nrow(new_rows)))
  }
  
  return(full_df)
}

result <- combine_parquet_sequential("D:\\sme-support\\Data\\out_of_parsing")
write_parquet(result, "D:\\sme-support\\Data\\Support\\msppp_constructed.parquet")

# result_dedupl <- result %>% distinct()
