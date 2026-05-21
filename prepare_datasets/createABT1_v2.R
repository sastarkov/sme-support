library(arrow)
library(dplyr)
library(data.table)
library(skimr)
library(corrplot)

# library(fixest)
# library(AER)

#0.Собираем датасет для расчета зависимых и инструментальных переменных  
abt <- open_dataset("D:\\sme-support\\Data\\ABT\\ABT0_1") |> 
  select(
    inn, 
    year, 
    okved_section, 
    region = region_taxcode, 
    okved, 
    age, 
    dissolution_date, 
    months_new, 
    months_social, 
    headcount,  
    cat = prefer_category, 
    lic = has_license,
    fin, 
    cap, 
    prom, 
    optim, 
    reduce, 
    type_406, 
    revenue = line_2110,
    net_profit = line_2400,
    current_assets = line_1200,
    inventories = line_1210,
    fund = line_1300,
    current_liabilities = line_1500,
    assets = line_1600) |> 
  collect() |> 
  as.data.table()

#Чистка строковых переменных
char_cols <- c("inn", "region", "okved", "cat", "okved_section")
abt[, (char_cols) := lapply(.SD, function(x) trimws(as.character(x))), .SDcols = char_cols]

# приведение типа к int
abt[, dissolution_date := as.integer(dissolution_date)]

#1.УДАЛЕНИЕ НЕКОРРЕКТНЫХ ЗНАЧЕНИЙ И СОЗДАНИЕ ПРОИЗВОДНЫХ ПЕРЕМЕННЫХ 

# преобразуем переменные, содержащие продолжительность пребывания в статусе социального предприятия и нового хотя бы один месяц в году
abt[months_new > 0, months_new := 1][, new := months_new == 1][, months_new := NULL]
abt[months_social > 0, months_social := 1][, social := months_social == 1][, months_social := NULL]

#Удаляем записи с пропусками в столбцах для расчета инструментов
abt <- abt[!is.na(okved_section) & !is.na(region)]

#Удаляем записи с отрицательными значениями ССЧР
abt <- abt[headcount >= 0 | is.na(headcount)]

#Удаляем записи с некорректными значениями ("мусор")
abt <- abt[assets > 0 | is.na(assets)] # в балансе активы не могут быть нулевыми или отрицательными, нулевые активы - неживые фирмы (ни дебиторки, ни денег на оплату ресурсов)
abt <- abt[current_assets <= assets | is.na(current_assets)] # Оборотные активы не могут больше активов

abt <- abt[inventories >= 0 | is.na(inventories)] # запасы не могут быть отрицательными
abt <- abt[inventories <= current_assets | is.na(inventories)] # запасы составная часть оборотных активов

abt <- abt[current_liabilities >= 0 | is.na(current_liabilities)] # Долги. Отрицательный долг в пассиве невозможен (это была бы дебиторская задолженность в активе)

abt <- abt[revenue >= 0 | is.na(revenue)] # фирмы, которые работают и дают вклад

# исключаем фирмы с отрицательным капиталом, которые могут быть похожи на zombie и стартапы (блокирование обратной связи) или неизвестной доли
abt <- abt[fund > 0] 

# 2 РАСЧЕТ СОСТАВНЫХ ПРИЗНАКОВ

#2.1 Расчет переменной для контроля масштаба - hc_last: прошлогодние активы, если есть, если нет - текущие

abt[, year := as.integer(as.character(year))] # 1. Принудительно превращаем в числа (удаляя возможные пробелы)
abt[, headcount := as.numeric(as.character(headcount))]
#Сортируем (критично для корректного сдвига shift)
setkey(abt, inn, year)
#Создаем переменную контроля масштаба
abt[, hc_last := {
  # Считаем текущий логарифм
  # Используем pmax(assets, 1), чтобы избежать log(0) = -Inf
  curr_log = log(pmax(headcount, 1))
  # Считаем лаг (прошлый год)
  lag_log = shift(curr_log, n = 1, type = "lag")
  # Если лаг — NA (пусто), берем текущий. Если оба NA — останется NA.
  fcoalesce(lag_log, curr_log)
}, by = inn]
# удалим данные, где невозможен контроль масштаба hc_last = NA
abt <- abt[!is.na(hc_last)]

#2.2 Расчет зависимых переменных

abt[, ln_assets := log1p(assets)]
abt[, ln_revenue := log1p(revenue)]
abt[, ash_profit := asinh(net_profit)]

#2.3 Считаем финансовые коэффициенты эффективности и устойчивости 
abt[, `:=`(

  # ros = net_profit / revenue,                                                         # рентабельность продаж
  # roa = net_profit / avg_assets,                                                      # рентабельность активов

  eq_flex   = (current_assets - current_liabilities) / fund,                      # коэффициент маневренности
  inv_eq = (current_assets - current_liabilities)/inventories,                    # коэффициент обеспеченности запасов собственными источниками
  aut      = fund / assets                                                       # Коэффициент независимости
  # debt_eq    = (assets - fund) / fund                                             # Коэффициент соотношения заемных и собственных средств
)]

#2.4 оставляем только строки, которые позволяют рассчитать хотя бы что - то одно (значение хотя бы одного столбца не NA)

cols_to_check2 <- c("ln_assets", "ln_revenue", "ash_profit", 
"eq_flex", "inv_eq", "aut")
abt <- abt[abt[, Reduce(`|`, lapply(.SD, function(x) !is.na(x))), .SDcols = cols_to_check2]]

#3 Винзоризация коэффициентов

# skim(abt)

#Удаляем записи с ошибочными значениями коэффициентов при неотрицательном капитале
# abt <- abt[autonomy >=0 & autonomy <= 1 | is.na(autonomy)] # [0; 1]
# abt <- abt[debt_equity >=0 | is.na(debt_equity)] # [0; +Inf]
# abt <- abt[equity_flex <= 1 | is.na(equity_flex)] # [-Inf; 1]

#Clipping
abt[inv_eq > 20, inv_eq := 20]
abt[inv_eq < -20, inv_eq := -20]
# abt[debt_equity > 15, debt_equity := 15]
# abt[is.nan(ros), ros := 0]

# abt[revenue == 0 & net_profit == 0, ros := 0] # фирмам, у которых не было продаж в этом году (выручка 0 и прибыль 0) принудительно назначим маржинальность бизнеса 0 (ros = 0)

#Проведем винзоризацию финансовых коэффициентов для устранения выбросов на уровне 5%, 95%
winsorize <- function(x, probs = c(0.05, 0.95)) {
  q <- quantile(x, probs, na.rm = TRUE)
  x[x < q[1]] <- q[1]
  x[x > q[2]] <- q[2]
  x
}

vars <- c("eq_flex", "inv_eq", "aut")
abt[, (vars) := lapply(.SD, winsorize), .SDcols = c("eq_flex", "inv_eq", "aut")]
# skim(abt)

#Создаем инструментальные переменные для поддержек 

#Список целевых переменных поддержки
# support_vars <- c("injections", "finance", "promotion", "management", "education", "innovation")

# #Создаем агрегированную таблицу по Региону, Отрасли и Году (cчитаем сумму поддержек и общее кол-во МСП в группе)
# regional_industry_stats <- abt[, .(
#   sum_injections = sum(injections, na.rm = TRUE),
#   sum_finance    = sum(finance, na.rm = TRUE),
#   sum_promotion  = sum(promotion, na.rm = TRUE),
#   sum_management = sum(management, na.rm = TRUE),
#   sum_education  = sum(education, na.rm = TRUE),
#   sum_innovation = sum(innovation, na.rm = TRUE),
#   n_firms        = .N
# ), by = .(region, okved, year)]

# #Присоединяем статистику обратно к основной панели abt
# abt <- merge(abt, regional_industry_stats, 
#              by = c("region", "okved", "year"), 
#              all.x = TRUE)

# #Вычисляем инструменты (IV) с использованием leave-one-out (Сумма поддержек в группе - поддержка текущей фирмы) / (Всего фирм в группе - 1)
# for (var in support_vars) {
#   sum_col <- paste0("sum_", var)
#   iv_name <- paste0("iv_", var)
  
#   abt[, (iv_name) := (get(sum_col) - get(var)) / (n_firms - 1)]
  
#   # Если в группе была всего 1 фирма, получится NaN. Заменяем на 0.
#   abt[is.nan(get(iv_name)) | is.infinite(get(iv_name)), (iv_name) := 0]
# }

# #Удаляем промежуточные колонки сумм, чтобы не загромождать таблицу
# cols_to_remove <- c(paste0("sum_", support_vars), "n_firms")
# abt[, (cols_to_remove) := NULL]

#2.ПРОВЕРКА КОРРЕЛЯЦИЙ ПЕРЕМЕННЫХ

#Готовим датасет в итогов виде

abt[, c("current_assets", "inventories", "fund", "current_liabilities", "assets", "headcount", "revenue", "net_profit") := NULL]

# Создаем список переменных (только те, что будут в регрессиях)
core_vars <- c("age", "lic", "fin", "cap", "prom", "optim", "reduce", 
"type_406", "new", "social", "hc_last", "ln_assets", "ln_revenue", "ash_profit", 
"eq_flex", "inv_eq", "aut")

# Считаем матрицу
cor_y <- cor(abt[, ..core_vars],  use = "pairwise.complete.obs")

# Выводим в консоль
round(cor_y, 2)

corrplot(cor_y, method = "circle", type = "lower", 
         tl.cex = 0.7, tl.col = "black")

#3.СОХРАНЕНИЕ ДАТАСЕТА
write_dataset(abt, 
    path = "D:\\sme-support\\Data\\TABLES\\ABT1_5",
    partitioning = "year",
    format = "parquet",
    existing_data_behavior = "delete_matching")
skim(abt)
# glimpse(abt)