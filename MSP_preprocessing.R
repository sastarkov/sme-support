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


aggregate_msp <- function(ds) {

  query_aggr <- ds %>%
    group_by(inn, year) %>%
    summarise(
      # Первый и последний месяц присутствия фирмы в данном году
      first_month = min(month, na.rm = TRUE),
      last_month  = max(month, na.rm = TRUE),
      
      # Самая ранняя дата включения в реестр (не зависит от года)
      previous_start_date = min(inclusion_date, na.rm = TRUE),

      region = last(region, order_by = month),
      
      # Список из двух чисел: число месяцев с категорией 1 и 2
      category_counts = list(c(
        sum(category == '1', na.rm = TRUE),
        sum(category == '2', na.rm = TRUE)
      )),
      
      # Направление тренда категории: 1 – рост, -1 – снижение, 0 – без изменений
      category_trend = case_when(
        last(category, order_by = date) > first(category, order_by = month) ~ 1L,
        last(category, order_by = date) < first(category, order_by = month) ~ -1L,
        TRUE ~ 0L
      ),
      
      # Количество месяцев с признаком "новое предприятие" (new == "1")
      count_new = sum(sign_new == "1", na.rm = TRUE),
      
      # Количество месяцев с признаком "социальное предприятие" (social == "1")
      count_social = sum(sign_social == "1", na.rm = TRUE),
      
      # Средняя численность за первое (мес. 1–6) и второе (мес. 7–12) полугодие
      headcount_1h = mean(ifelse(month %in% 1:6, headcount, NA), na.rm = TRUE),
      headcount_2h = mean(ifelse(month %in% 7:12, headcount, NA), na.rm = TRUE),
      
      # Последнее известное значение основного ОКВЭД
      main_okved = last(main_okved, order_by = month),
      
      # Последний известный список дополнительных ОКВЭД
      other_okved = last(other_okved, order_by = month),
      
      # Количество месяцев, когда у фирмы была лицензия (license == 1)
      count_license = sum(license == '1', na.rm = TRUE),
      
      .groups = "drop"
    )

  return(query_aggr)
  
}

df <- open_dataset("MSP_parsed") %>%   
 filter(year == 2017) %>% 
 filter(category == 3) %>% 
 aggregate_msp() %>% 
 collect()


# df_aggr <- df %>%
#   group_by(inn) %>%
#   summarise(
#     first_month = min(month),
#     last_month = max(month),
#     quantity_month = n(),
#     quantity_date_inclusion = n_distinct(inclusion_date)
#   )