library(magrittr)
library(glue)
library(zeallot)
languages <- readr::read_csv("language_codes.csv", col_types = "cc")

possible_wikis <- expand.grid(
  code = unique(languages$code),
  suffix = c("", "b", "d", "m", "mw", "s", "n", "q", "v", "voy"),
  stringsAsFactors = FALSE
) %>%
  dplyr::mutate(
    wiki = sub("(.*)\\.$", "\\1", paste(code, suffix, sep = ".")),
    project = as.character(factor(
      suffix,
      c("", "b", "d", "mw", "s", "n", "q", "v", "voy"),
      c("wikipedia", "wikibooks", "wiktionary", "wikipedia (mobile)", "wikisource", "wikinews", "wikiquote", "wikiversity", "wikivoyage")
    ))
  )

file_paths <- c(
  fs::dir_ls(
    path = "/mnt/hdfs/wmf/data/archive/projectcounts-raw",
    recurse = TRUE,
    regexp = "\\.gz$"
  ),
  fs::dir_ls(
    path = "/mnt/hdfs/wmf/data/archive/projectcounts-all-sites",
    recurse = TRUE,
    regexp = "\\.gz$"
  )
)
file_paths <- dplyr::tibble(path = file_paths) %>%
  dplyr::arrange(path)

projectcounts <- purrr::map_dfr(
  file_paths$path,
  function(file_path, dataset) {
    message("Reading ", file_path)
    year <- sub(".*year=([0-9]{4}).*", "\\1", fs::path_dir(file_path))
    data <- sub(".*\\/projectcounts-((raw)|(all-sites)).*", "\\1", file_path)
    # note: mobile data available as of 23 sept 2014, but only in all-sites
    projectcounts <- file_path %>%
      readr::read_table2(col_types = "iiici", col_names = c("month", "day", "hour", "wiki", "requests")) %>%
      dplyr::filter(wiki %in% possible_wikis$wiki) %>%
      dplyr::mutate(year = year, dataset = data) %>%
      dplyr::group_by(dataset, wiki, year, month, day) %>%
      dplyr::summarize(requests = sum(requests)) %>%
      dplyr::ungroup()
    return(projectcounts)
  }
)

projectcounts <- projectcounts %>%
  dplyr::left_join(possible_wikis[, c("wiki", "code", "project")], by = "wiki") %>%
  tidyr::spread(dataset, requests, fill = NA) %>%
  dplyr::rename(pagecounts_all_sites = `all-sites`, pagecounts_raw = raw) %>%
  dplyr::arrange(year, month, day, wiki)

mobile_wikipedia <- projectcounts %>%
  dplyr::filter(project == "wikipedia (mobile)") %>%
  dplyr::rename(
    pagecounts_all_sites_mobile = pagecounts_all_sites,
    pagecounts_raw_mobile = pagecounts_raw
  ) %>%
  dplyr::mutate(project = "wikipedia") %>%
  dplyr::select(-wiki)

projectcounts <- projectcounts %>%
  dplyr::filter(project != "wikipedia (mobile)") %>%
  dplyr::left_join(mobile_wikipedia, by = c("year", "month", "day", "code", "project"))

projectcounts <- projectcounts %>%
  dplyr::mutate(
    year = as.integer(year),
    month = as.numeric(month),
    day = as.numeric(day),
    date = as.Date(sprintf("%i-%02.0f-%02.0f", year, month, day))
  ) %>%
  dplyr::select(-c(year, month, day)) %>%
  dplyr::select(date, wiki, code, project, dplyr::everything())

object.size(projectcounts) # 223575792 bytes, a tibble: 4,655,157 x 8
readr::write_csv(projectcounts, "projectcounts.csv")
system("gzip projectcounts.csv") # 40M
