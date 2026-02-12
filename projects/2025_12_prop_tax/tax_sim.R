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
ptaxsim_db_conn <- DBI::dbConnect(
  RSQLite::SQLite(),
  here("data/input/ptaxsim.db")
)

# Grab the yearly equalizer for determining EAV
eq_df <- DBI::dbGetQuery(
  ptaxsim_db_conn,
  "
  SELECT *
  FROM eq_factor
  "
)


##### MacNeal tax simulation ###################################################

# Load MacNeal hospital PINs pulled from Socrata API
macneal_pins_lst <- read_csv(
  "data/intermediate/macneal_pins_confirmed.csv",
  col_types = "cc"
)$pin

# Grab MacNeal PIN assessed values before and after the acquisition
macneal_pins_df <- lookup_pin(
  year = 2019:2020,
  pin = macneal_pins_lst
)

# Grab the actual billed values for the MacNeal PINs
macneal_bills_actual_df <- tax_bill(
  year_vec = 2015:2023,
  pin_vec = macneal_pins_lst,
  simplify = TRUE
)

macneal_bills_actual_df |>
  write_parquet("data/output/macneal_tax_bills.parquet")

# Grab all the PINs in Berwyn township, excluding MacNeal.These are the used
# for our counterfactual i.e. the PINs most affected by the MacNeal sale
berwyn_pins_lst <- DBI::dbGetQuery(
  ptaxsim_db_conn,
  "
  SELECT p.pin
  FROM pin AS p
  LEFT JOIN tax_code AS tc
      ON p.tax_code_num = tc.tax_code_num
      AND p.year = tc.year
  LEFT JOIN agency AS a
      ON tc.year = a.year
      AND tc.agency_num = a.agency_num
  WHERE a.agency_num = '020020000'
      AND a.year = 2019
  "
) |>
  filter(!pin %in% macneal_pins_lst) |>
  pull(pin)

# Grab all Berwyn PIN assessed values before and after the acquisition
berwyn_pins_df <- lookup_pin(
  year = 2019:2020,
  pin = berwyn_pins_lst
)

# Grab all the actual bills from Berwyn PINs before and after the MacNeal sale
berwyn_bills_actual_df <- tax_bill(
  2019:2020,
  berwyn_pins_lst
)

# Calculate the YoY AV change in Berwyn between 2019 and 2020. We'll use this to
# determine the counterfactual value of the MacNeal PINs
berwyn_av_pct_changes_df <- berwyn_pins_df |>
  pivot_wider(id_cols = c(pin, class), names_from = year, values_from = av) |>
  mutate(major_class = substr(class, 1, 1)) |>
  mutate(pct_change = (`2020` - `2019`) / `2019`) |>
  group_by(major_class) |>
  summarize(avg_pct_change = mean(pct_change, na.rm = TRUE))

macneal_eav_counter_df <- macneal_pins_df |>
  filter(year == 2019) |>
  mutate(major_class = substr(class, 1, 1)) |>
  select(-starts_with("exe_")) |>
  left_join(
    macneal_pins_df |>
      filter(year == 2020) |>
      select(pin, av_2020_actual = av),
    by = "pin"
  ) |>
  left_join(berwyn_av_pct_changes_df, by = "major_class") |>
  cross_join(eq_df |> filter(year == 2020) |> select(eq_factor_final)) |>
  mutate(
    av_2020_counter = round(av + (av * avg_pct_change)),
    av_2020_counter = ifelse(
      av_2020_actual != 0,
      av_2020_actual,
      av_2020_counter
    ),
    tax_code = lookup_tax_code(2020, pin)
  ) |>
  group_by(tax_code) |>
  summarize(
    total_eav_2020_actual = sum(av_2020_actual * eq_factor_final),
    total_eav_2020_counter = sum(
      av_2020_counter * eq_factor_final,
      na.rm = TRUE
    ),
    eav_diff = bit64::as.integer64(round(
      total_eav_2020_counter - total_eav_2020_actual
    ))
  ) |>
  mutate(year = 2020) |>
  filter(eav_diff > 0) |>
  select(year, tax_code, eav_diff) |>
  inner_join(
    berwyn_bills_actual_df |> distinct(tax_code, agency_num, agency_name),
    by = "tax_code"
  )

berwyn_counter_agency_dt <- lookup_agency(
  2019:2020,
  unique(berwyn_bills_actual_df$tax_code)
) |>
  left_join(
    macneal_eav_counter_df |> select(year, agency_num, eav_diff),
    by = c("year", "agency_num")
  ) |>
  mutate(
    across(c(agency_total_eav, eav_diff), \(x) as.numeric(x)),
    agency_total_eav = rowSums(pick(agency_total_eav, eav_diff), na.rm = TRUE)
  ) |>
  select(-eav_diff) |>
  setDT(key = c("year", "tax_code", "agency_num"))

berwyn_bills_counter_df <- tax_bill(
  year_vec = 2019:2020,
  pin_vec = berwyn_pins_lst,
  agency_dt = berwyn_counter_agency_dt
)

bill_diff_df <- berwyn_bills_actual_df |>
  group_by(pin, year) |>
  summarize(actual_tax = sum(final_tax, na.rm = TRUE)) |>
  left_join(
    berwyn_bills_counter_df |>
      group_by(pin, year) |>
      summarize(counter_tax = sum(final_tax, na.rm = TRUE))
  ) |>
  mutate(diff = actual_tax - counter_tax) |>
  filter(actual_tax > 0)

bill_diff_df |>
  write_parquet("data/output/tax_bill_diff.parquet")


##### Grab PIN shapes ##########################################################

berwyn_pin_geos <- lookup_pin10_geometry(
  year = 2023,
  pin10 = substr(c(berwyn_pins_lst, macneal_pins_lst), 1, 10)
) |>
  mutate(macneal_pin = pin10 %in% substr(macneal_pins_lst, 1, 10)) |>
  left_join(
    macneal_bills_actual_df |>
      filter(year %in% 2019:2020) |>
      mutate(pin10 = substr(pin, 1, 10)) |>
      group_by(pin10, year) |>
      summarize(final_tax = sum(final_tax)) |>
      pivot_wider(names_from = year, values_from = final_tax) |>
      mutate(became_exempt = `2019` != 0 & `2020` == 0) |>
      select(pin10, became_exempt),
    by = "pin10"
  )

berwyn_pin_geos |>
  write_parquet("data/output/berwyn_pin_geos.parquet")
