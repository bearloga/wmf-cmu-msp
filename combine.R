library(magrittr)
languages <- readr::read_csv("language_codes.csv", col_types = "cc")

modern_pageviews <- readr::read_csv(
  "modern_pageviews.csv.gz",
  col_types = "cDcccii"
)
legacy_pageviews <- readr::read_csv(
  "projectcounts.csv.gz",
  col_types = "Dccciiii"
) %>% dplyr::left_join(languages, by = "code")

combined_traffic <- dplyr::full_join(
  dplyr::select(legacy_pageviews, -wiki),
  dplyr::select(modern_pageviews, -wiki),
  by = c("date", "code" = "sub_domain", "project", "language")
)

combined_traffic <- combined_traffic %>%
  dplyr::select(date, project, code, language, dplyr::everything()) %>%
  dplyr::arrange(date, code, project)

object.size(combined_traffic) # 277003800 bytes; a tibble: 4,945,819 x 10

readr::write_csv(combined_traffic, "handoff/combined_traffic.csv")
system("gzip -f handoff/combined_traffic.csv")

# overview
library(ggplot2)
library(hrbrthemes)

tidy_traffic <- combined_traffic %>%
  tidyr::gather(data, views, -c(date, project, code, language))

traffic_overview <- tidy_traffic %>%
  dplyr::group_by(date, data) %>%
  dplyr::summarize(views = sum(views, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(data = factor(data, c("modern_pageviews_desktop", "pagecounts_raw", "pagecounts_all_sites", "modern_pageviews_mobile", "pagecounts_raw_mobile", "pagecounts_all_sites_mobile")))

ggplot(traffic_overview, aes(x = date, y = views)) +
  geom_line(
    aes(group = tmp), color = "gray", size = 0.25,
    data = dplyr::rename(traffic_overview, tmp = data)
  ) +
  geom_line(aes(color = data)) +
  facet_wrap(~ data, ncol = 3) +
  scale_color_brewer(palette = "Dark2", guide = FALSE) +
  scale_y_continuous(labels = polloi::compress) +
  theme_ipsum_rc() +
  labs(x = "Date", y = "Pageviews/day", title = "Overview of traffic counts")

