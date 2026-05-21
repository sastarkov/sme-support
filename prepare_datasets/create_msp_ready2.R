library(arrow)
library(dplyr)
library(data.table)


schema_msp <- schema(inn = string(), months = int32(), prefer_category = string(), months_new = int32(), 
months_social = int32(), has_license = bool(), headcount = double(), year = int32())

ds_msp <- open_dataset("D:\\sme-support\\Data\\MSP_ready", schema = schema_msp) |> 
  # select(-c(previous_start_date)) |> 
  filter(year >= 2018, year <= 2024)

schema(ds_msp)

dt_msp <- ds_msp |> 
  collect() |>
  as.data.table()

# преобразуем переменные, содержащие продолжительность пребывания в статусе социального предприятия и нового хотя бы один месяц в году
dt_msp[months_new > 0, months_new := 1][, new := months_new == 1][, months_new := NULL]
dt_msp[months_social > 0, months_social := 1][, social := months_social == 1][, months_social := NULL]

#3.СОХРАНЕНИЕ ДАТАСЕТА
write_dataset(dt_msp, 
    path = "D:\\sme-support\\Data\\Temp\\MSP_ready2",
    partitioning = "year",
    format = "parquet",
    existing_data_behavior = "delete_matching")

glimpse(dt_msp)
