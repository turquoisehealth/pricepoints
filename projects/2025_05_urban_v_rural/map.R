library(sf)
library(dplyr)
library(readr)
library(ggplot2)

final_df <- read_csv("final.csv")
final_df <- final_df %>% st_as_sf(wkt = "geometry", crs = 4326) %>% st_transform(9311)

final_df %>%
  filter(
    !substr(geoid, 1, 2) %in% c("02", "15", "72"),
    substr(geoid, 1, 2) <= "56"
  ) %>%
ggplot() +
  geom_sf(aes(fill = no_provider, color = no_provider)) +
  scale_color_manual(values = c("FALSE" = "#fc8d62", "TRUE" = "grey90")) +
  scale_fill_manual(values = c("FALSE" = "#fc8d62", "TRUE" = "grey90")) +
  theme_void() +
  labs(title = "Orange = census tracts with an acute care or critical access hospital more than 30 minutes away") +
  theme(
    legend.position = "none",
    plot.title = element_text(size = 14, margin = margin(l = 30, b = 4))
  )
