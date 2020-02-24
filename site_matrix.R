library(jsonlite)
library(purrr)

url <- "https://www.mediawiki.org/w/api.php?action=sitematrix&format=json&smtype=language&formatversion=2"
json <- fromJSON(url, simplifyVector = FALSE)

language_codes <- map_dfr(
  json$sitematrix[-1], # first element is $count
  function(language) {
    if ("localname" %in% names(language)) lang <- language$localname
    else lang <- as.character(NA)
    if ("code" %in% names(language)) code <- language$code
    else code <- as.character(NA)
    return(dplyr::tibble(language = lang, code = code))
  }
)

readr::write_csv(language_codes, "language_codes.csv")
