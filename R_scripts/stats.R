library(arrow)
library(dplyr)
# library(ExPanDaR)
# library(vtable)
# library(summarytools)


# Загружаем датасет 
# df_test <- open_dataset("Data/MSP_ready", partitioning = "year") %>% 
# sample_n(1000) %>% 
# collect()


# ExPanD(df_test, ts_id = "year", cs_id = "inn")

# sumtable(df_test, group = 'prefer_category', group.long = TRUE)

diff_ds <- function(ds1_path, ds2_path) {

  ds1 <- open_dataset(ds1_path) %>% collect()
  ds2 <- open_dataset(ds2_path) %>% collect()

  only_in_ds1 <- anti_join(ds2, ds1, by = "inn")
  
  return(only_in_ds1)
} 

df <- diff_ds("Data\\out_of_parsing\\sschr.parquet", "Data\\MSP_ready\\year=2024\\part-0.parquet")




