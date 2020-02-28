library(magrittr)

languages <- readr::read_csv("language_codes.csv", col_types = "cc")

modern_pageviews <- "backup-modern/monthly" %>%
  fs::dir_ls(regexp = "\\.csv\\.gz") %>%
  purrr::map_dfr(readr::read_csv, col_types = "ccciiii") %>%
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
  dplyr::mutate(
    access_method = ifelse(access_method == "desktop", "modern_desktop", "modern_mobile"),
    agent_type = ifelse(agent_type == "user", "user", "bot")
  ) %>%
  tidyr::pivot_wider(
    names_from = c(access_method, agent_type),
    values_from = view_count,
    values_fill = list(view_count = 0)
  ) %>%
  dplyr::mutate(
    modern_desktop_all = modern_desktop_user + modern_desktop_bot,
    modern_mobile_all = modern_mobile_user + modern_mobile_bot
  ) %>%
  dplyr::arrange(date, wiki)

modern_pageviews %>% head
readr::write_csv(modern_pageviews, "modern_pageviews.csv")
system("gzip modern_pageviews.csv")
