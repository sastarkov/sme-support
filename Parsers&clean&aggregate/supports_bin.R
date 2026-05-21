library(arrow)
library(dplyr)
library(lubridate)
library(tidyr)
library(corrplot)
library(car)
library(data.table)

#!!!!!!!! ДАТАСЕТ СО СЧЕТЧИКАМИ ПОДДЕРЖКИ И ПРЕДМЕТНОЙ АГРЕГАЦИЕЙ

# базовый датасет для мер поддержки
supp_base <- open_dataset("D:\\sme-support\\Data\\Support\\msppp_constructed.parquet") %>%
    select(inn, date_supp, date_decision_support, support_type, support_amount) %>%
  distinct() %>%  # удалим технические дубликаты
  mutate(year = as.integer(year(ymd(date_decision_support)))) %>%
  mutate(year_end = as.integer(year(ymd(date_supp)))) %>%
  select(-c(date_supp, date_decision_support)) %>%
  filter(support_type != "0106") %>%
  collect() %>% 
  drop_na() %>%
  as.data.table()

# 1. Выделяем записи относящиеся к переменной fin
supp_fin <- supp_base[support_type %in% c("0102", "0103", "0104")]
supp_fin <- supp_fin[, .(support_amount = .N,
                         support = "fin"), 
                     by = .(inn, support_type, year)]


# 2. Выделяем записи относящиеся к переменной cap
supp_cap <- supp_base[support_type %in% c("0101", "0105", "0407", "0501", "0503", "0506", "0507", "0508", "0601", "0602")]
supp_cap[, support := "cap"]
supp_cap <- supp_cap[, .(support_amount = .N), by = .(inn, support_type, year, support)]


# 3. Выделяем записи относящиеся к переменной prom
supp_prom <- supp_base[support_type %in% c("0201", "0202", "0203", "0402", "0404", "0410", "0411", "0413", "0502"), 
                            .(support_amount = .N, 
                              support = "prom"), 
                            by = .(inn, support_type, year)]


# 4. Выделяем записи относящиеся к переменной optim
supp_optim <- supp_base[support_type %in% c("0204", "0301", "0302", "0303", "0405", "0408", "0409", "0412", "0414", "0415", "0416", "0417"),
                             .(support_amount = .N, 
                               support = "optim"), 
                             by = .(inn, support_type, year)]


# 5. Выделяем записи относящиеся к переменной reduce
supp_reduce <- supp_base[support_type %in% c("0205", "0206", "0504", "0505", "0603"), 
                            .(support_amount = .N, 
                              support = "reduce"), 
                            by = .(inn, support_type, year)]

#выделим отдельно type_0406
supp_406 <- supp_base[support_type %in% c("0406", "0401", "0403"), 
                            .(support_amount = .N, 
                              support = "type_406"), 
                            by = .(inn, support_type, year)]


# склеиваем все в обратно в одну таблицу (с количественными данными)
supp_base_continuous <- rbindlist(list(supp_fin, supp_cap, supp_prom, supp_optim, supp_reduce, supp_406), use.names = TRUE)


#КОЛИЧЕСТВЕННЫЕ МЕРЫ ПОДДЕРЖКИ
# подготовим таблицу в широком формате с количественными данными
support_cont_full <- copy(supp_base_continuous)
support_cont_full[, support := NULL] 
support_cont_full <- support_cont_full |> 
  pivot_wider(
    names_from = support_type,      # Значения из этой колонки станут названиями столбцов
    values_from = support_amount,    # Значения из этой колонки заполнят ячейки
    values_fill = 0,                 # Чем заменить отсутствующие комбинации (NA)
    names_prefix = "type_"
  )

setDT(support_cont_full)

support_cont_full$injections <- support_cont_full$type_0101 + support_cont_full$type_0103 + 
support_cont_full$type_0501 + support_cont_full$type_0503 + support_cont_full$type_0506 + 
support_cont_full$type_0507 + support_cont_full$type_0508

support_cont_full$finance <- support_cont_full$type_0102 + support_cont_full$type_0104 + 
support_cont_full$type_0105 + support_cont_full$type_0504 + support_cont_full$type_0505

support_cont_full$promotion <- support_cont_full$type_0201 + support_cont_full$type_0202 + 
support_cont_full$type_0203 + support_cont_full$type_0204 + support_cont_full$type_0402 + 
support_cont_full$type_0410 + support_cont_full$type_0411 + support_cont_full$type_0413 + 
support_cont_full$type_0502

support_cont_full$management <- support_cont_full$type_0205 + support_cont_full$type_0206 + 
support_cont_full$type_0401 + support_cont_full$type_0403 + support_cont_full$type_0405 + 
support_cont_full$type_0407 + support_cont_full$type_0409 + support_cont_full$type_0412 + 
support_cont_full$type_0414 + support_cont_full$type_0416 + support_cont_full$type_0417

support_cont_full$education <- support_cont_full$type_0301 + support_cont_full$type_0302 + 
support_cont_full$type_0303 + support_cont_full$type_0415

support_cont_full$innovation <- support_cont_full$type_0404 + support_cont_full$type_0408 + 
support_cont_full$type_0601 + support_cont_full$type_0602 + support_cont_full$type_0603

support_counts <- support_cont_full |> 
  select(c(inn, year, injections, finance, promotion, management, education, innovation, type_0406)) |> 
  filter(year >= 2018 & year <= 2024)

write_parquet(support_counts, "D:\\sme-support\\Data\\Support\\support_continuous.parquet")

#КАЧЕСТВЕННЫЕ МЕРЫ ПОДДЕРЖКИ
# подготовим таблицу в широком формате с бинарными данными

support_binary <- copy(supp_base_continuous)
support_binary[, support_amount := 1]
support_binary[, support_type := NULL]
support_binary <- unique(support_binary)

support_binary <- support_binary |> 
  pivot_wider(
    names_from = support,      # Значения из этой колонки станут названиями столбцов
    values_from = support_amount,    # Значения из этой колонки заполнят ячейки
    values_fill = 0,                 # Чем заменить отсутствующие комбинации (NA)
  )
setDT(support_binary)

support_binary <- support_binary |>  
  filter(year >= 2019 & year <= 2024)

write_parquet(support_binary, "D:\\sme-support\\Data\\Support\\support_binary_final.parquet")
