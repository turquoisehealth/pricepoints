library(arrow)
library(conflicted)
library(data.table)
library(dplyr)
library(here)
library(ptaxsim)
library(readr)
library(scales)
library(stringr)
library(tidyr)

# Always use dplyr over other packages
conflict_prefer_all("dplyr", quiet = TRUE)

# Connect to PTAXSIM database, downloaded from:
# https://github.com/ccao-data/ptaxsim
ptaxsim_db_conn <- DBI::dbConnect(RSQLite::SQLite(), here("ptaxsim.db"))

# Load Cook PINs pulled from Socrata API
cook_address <- read_csv("data/intermediate/cook_address.csv", col_types = "cc")

temp <- tax_bill(year_vec = 2010:2023, pin_vec = cook_address$pin) |>
  group_by(pin, year) |>
  summarize(final_tax = sum(final_tax)) |>
  arrange(pin, year) |>
  pivot_wider(names_from = year, values_from = final_tax)

group_by(year) |>
  summarize(total_tax = sum(final_tax))
