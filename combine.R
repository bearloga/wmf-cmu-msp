library(magrittr)
languages <- readr::read_csv("language_codes.csv", col_types = "cc")

modern_pageviews <- readr::read_csv(
  "modern_pageviews.csv.gz",
  col_types = "cDccciiii"
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
