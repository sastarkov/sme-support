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

source(here("R", "MSP_preprocessing.R"))

tic('Время выполнения:')

find_dupl("MSP_parsed", 2017:2024)

toc()