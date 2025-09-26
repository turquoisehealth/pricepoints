# type: ignore
# %% Import Python libraries and set up Trino
import openpyxl as px
import polars as pl
from openpyxl.utils.dataframe import dataframe_to_rows
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()

MIN_N_PAYERS = 2
MIN_RATE_SCORE = 3


# Automatically set the column width in Excel sheets based on the content
def set_column_width(sheet, column, max_width):
    max_length = 0
    column_letter = column[0].column_letter
    for cell in column:
        try:
            if (
                len(str(cell.value)) > max_length
                and len(str(cell.value)) < max_width
            ):
                max_length = len(str(cell.value))
        except AttributeError:
            pass
    adjusted_width = max_length + 2
    sheet.column_dimensions[column_letter].width = adjusted_width


# %% Grab hand-picked codes and attach revenue
codes_df = pl.read_csv(
    "data/input/codes.csv", dtypes={"billing_code": pl.String}
)

# %% For each code, grab code coverage across hospitals and payer networks

# Convert the Polars table to a SQL VALUES clause
codes_values = ",".join(
    f"('{row['billing_code_type']}', '{row['billing_code']}')"
    for row in codes_df.to_dicts()
)
codes_table = f"(VALUES {codes_values}) AS t(billing_code_type, billing_code)"

# Fetch the count of providers that have at least two payers for each code
with open("queries/code_counts.sql", "r") as query_file:
    sql_template = query_file.read()
    sql = (
        sql_template.replace("{{ codes_table }}", codes_table)
        .replace("{{ min_n_payers }}", str(MIN_N_PAYERS))
        .replace("{{ min_rate_score }}", str(MIN_RATE_SCORE))
    )
    code_counts_df = pl.read_database(sql, trino_conn)
    code_counts_df = code_counts_df.with_columns(
        (pl.col("providers_w_gte_n_payers") / pl.col("total_providers")).alias(
            "percent_providers_w_n_payers"
        )
    )

with open("queries/payer_provider_counts.sql", "r") as query_file:
    sql_template = query_file.read()
    sql = (
        sql_template.replace("{{ codes_table }}", codes_table)
        .replace("{{ min_n_payers }}", str(MIN_N_PAYERS))
        .replace("{{ min_rate_score }}", str(MIN_RATE_SCORE))
    )

    # Fetch the SQL results and collapse all Blue Cross/Blue Shield payers
    provider_counts_df = pl.read_database(sql, trino_conn)
    provider_counts_df = provider_counts_df.with_columns(
        pl.when(
            pl.col("payer_name").str.contains("(?i)Blue Cross|Blue Shield")
        )
        .then(pl.lit("BCBS"))
        .otherwise(pl.col("payer_name"))
        .alias("payer_name")
    ).filter(
        pl.col("payer_name").is_in(
            ["BCBS", "Anthem", "United Healthcare", "Cigna", "Aetna"]
        )
    )

# %% Join code counts, then reorder and rename columns
codes_df = codes_df.join(
    code_counts_df, on=["billing_code_type", "billing_code"], how="left"
)
codes_df = codes_df.select(
    pl.col("billing_code_type").alias("Billing Code Type"),
    pl.col("billing_code").alias("Billing Code"),
    pl.col("service_line").alias("Service Line"),
    pl.col("service_description").alias("Service Description"),
    pl.col("providers_w_gte_n_payers").alias(
        f"Providers w >= {str(MIN_N_PAYERS)} Payers Per Code"
    ),
    pl.col("total_providers").alias("Providers Total"),
    pl.col("percent_providers_w_n_payers").alias(
        f"Percent Providers w >= {str(MIN_N_PAYERS)} Payers Per Code"
    ),
)
provider_counts_df = provider_counts_df.select(
    pl.col("provider_id").alias("Provider ID"),
    pl.col("provider_name").alias("Provider Name"),
    pl.col("payer_id").alias("Payer ID"),
    pl.col("payer_name").alias("Payer Name"),
    pl.col("payer_network_name").alias("Payer Network"),
    pl.col("state").alias("State"),
    pl.col("cbsa_name").alias("CBSA"),
    pl.col("cld_codes").alias("Codes w Rate"),
    pl.col("total_codes").alias("Codes Total"),
    pl.col("percent_coverage").alias("Percent Coverage"),
)

# %% Write the results to an Excel file
wb = px.Workbook()
ws1 = wb.active
ws1.title = "Codes"
for r in dataframe_to_rows(codes_df.to_pandas(), index=False, header=True):
    ws1.append(r)
# Set the header row to bold and adjust column widths
for cell in ws1[1]:
    cell.font = px.styles.Font(bold=True)
for col in ws1.columns:
    set_column_width(ws1, col, 60)
    if col[0].value == "Claims Revenue":
        for cell in col[1:]:
            cell.number_format = '"$"#,##0'

    if (
        col[0].value
        == f"Percent Providers w >= {str(MIN_N_PAYERS)} Payers Per Code"
    ):
        for cell in col[1:]:
            cell.number_format = "0.0%"

ws2 = wb.create_sheet("Counts by Payer-Provider")
for r in dataframe_to_rows(
    provider_counts_df.to_pandas(), index=False, header=True
):
    ws2.append(r)
for cell in ws2[1]:
    cell.font = px.styles.Font(bold=True)
for col in ws2.columns:
    set_column_width(ws2, col, 60)
    if col[0].value == "Percent Coverage":
        for cell in col[1:]:
            cell.number_format = "0.0%"

wb.save("data/output/codes_check.xlsx")
