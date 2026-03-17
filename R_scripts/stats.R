library(arrow)
library(dplyr)
library(knitr)
library(kableExtra)
library(skimr)

# Открываем датасет (лениво) и выбираем ключевые переменные
ds <- open_dataset("Data/RFSD", partitioning = "year")
# Теперь пробуем взять 10 строк
ds %>% head(10) %>% collect()

# df_sample %>% 
#   skim_without_charts() %>% 
#   yank("numeric") # выводит только числовые переменные

# # Определяем переменные, для которых хотим статистику
# vars <- c("age", "line_1600", "line_2400", "roa", "current_ratio") # пример

# # Вычисляем статистики на уровне всего датасета (лениво)
# stats <- ds %>%
#   select(all_of(vars)) %>%
#   summarise(across(
#     everything(),
#     list(
#       mean   = ~mean(.x, na.rm = TRUE),
#       sd     = ~sd(.x, na.rm = TRUE),
#       min    = ~min(.x, na.rm = TRUE),
#       max    = ~max(.x, na.rm = TRUE),
#       n      = ~sum(!is.na(.x)),
#       n_miss = ~sum(is.na(.x))
#     ),
#     .names = "{.col}_{.fn}"
#   )) %>%
#   collect()  # выполняем запрос и собираем результат в память

# # Транспонируем в удобный для чтения вид
# stats_long <- stats %>%
#   pivot_longer(everything(),
#                names_to = c("variable", "stat"),
#                names_sep = "_") %>%
#   pivot_wider(names_from = stat, values_from = value)

# # Округляем числовые значения для красоты
# stats_long <- stats_long %>%
#   mutate(across(where(is.numeric), ~round(.x, 2)))

# # Выводим таблицу
# kable(stats_long, caption = "Описательные статистики ключевых переменных") %>%
#   kable_styling(bootstrap_options = c("striped", "hover"), full_width = FALSE)