library(magrittr)

languages <- readr::read_csv("language_codes.csv", col_types = "cc")

modern_pageviews <- "backup-modern/monthly" %>%
  fs::dir_ls(regexp = "\\.csv\\.gz") %>%
  purrr::map_dfr(readr::read_csv, col_types = "cciiii") %>%
  dplyr::mutate(date = as.Date(sprintf("%i-%02.0f-%02.0f", year, month, day))) %>%
  dplyr::select(-c(year, month, day)) %>%
  dplyr::rename(wiki = project) %>%
  dplyr::mutate(
    sub_domain = sub("(.*)\\.(.*)", "\\1", wiki),
    project = sub("(.*)\\.(.*)", "\\2", wiki)
  ) %>%
  dplyr::filter(
    sub_domain %in% languages$code,
    project %in% c("wikibooks", "wikinews", "wikipedia", "wikiquote", "wikisource", "wikiversity", "wikivoyage", "wiktionary")
  ) %>%
  dplyr::left_join(languages, by = c("sub_domain" = "code")) %>%
  tidyr::spread(access_method, view_count, fill = 0) %>%
  dplyr::arrange(date, wiki) %>%
  dplyr::rename(modern_pageviews_desktop = desktop, modern_pageviews_mobile = `mobile web`)

readr::write_csv(modern_pageviews, "modern_pageviews.csv")
system("gzip modern_pageviews.csv")
