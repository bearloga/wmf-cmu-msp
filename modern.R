library(magrittr)
library(glue)
library(zeallot)

backup_dir <- "backup-modern"
fs::dir_create(fs::path(backup_dir, "monthly"))
projectview_root <- "/mnt/hdfs/wmf/data/wmf/projectview/hourly"
projectview_dir <- "{projectview_root}/year={year}/month={month}/day={day}/hour={hour}"
tmp_json <- fs::file_temp(tmp_dir = ".", ext = ".json")

start_date <- as.Date("2015-04-01")
end_date <- as.Date("2020-01-31")
dates <- seq(start_date, end_date, by = "day")

yearly_data <- purrr::map(unique(lubridate::year(dates)), function(year) {
  monthly_data <- purrr::map(
    unique(lubridate::month(dates[lubridate::year(dates) == year])),
    function(month) {
      if (fs::file_exists(file.path(backup_dir, "monthly", glue("{year}-{month}.csv.gz")))) {
        return(data.table::fread(file = file.path(backup_dir, "monthly", glue("{year}-{month}.csv.gz"))))
      }
      daily_data <- purrr::map(
        dates[lubridate::month(dates) == month & lubridate::year(dates) == year],
        function(date) {
          day <- lubridate::mday(date)
          message(glue("Processing modern pagecounts from {format(date)}"))
          hourly_data <- purrr::map(0:23, function(hour) {
            projectview_dir <- glue(projectview_dir)
            projectview_files <- fs::dir_ls(projectview_dir, regexp = "000000_0")
            segmented_data <- purrr::map(
              projectview_files,
              function(projectview_file) {
                # Convert from Parquet to JSON:
                system(glue("parquet-tools cat --json {projectview_file} > {tmp_json}"))
                # Read JSON into R:
                json_lines <- paste0("[", paste0(readr::read_lines(tmp_json), collapse = ","), "]")
                # Convert to data.frame:
                segment <- jsonlite::fromJSON(json_lines) # jsonify::from_json(json_lines)
                # Convert to data.table:
                segment <- data.table::data.table(segment)
                # Aggegate out non-relevant dimensions:
                segment <- segment[
                  access_method %in% c("desktop", "mobile web"),
                  list(view_count = sum(view_count)),
                  by = c("project", "access_method")
                ]
                # Set additional key columns:
                segment$year <- year
                segment$month <- month
                segment$day <- day
                segment$hour <- hour
                return(segment)
              })
            return(data.table::rbindlist(segmented_data))
          })
          return(data.table::rbindlist(hourly_data))
        })
      month_of_data <- data.table::rbindlist(daily_data)
      month_of_data <- month_of_data[, list(view_count = sum(view_count)), by = c("project", "access_method", "year", "month", "day")]
      message("Backing up to ", file.path(backup_dir, "monthly", glue("{year}-{month}.csv.gz")))
      data.table::fwrite(month_of_data, file = file.path(backup_dir, "monthly", glue("{year}-{month}.csv.gz")))
      return(month_of_data)
    })
  return(data.table::rbindlist(monthly_data))
})
projectview_data <- data.table::rbindlist(yearly_data)
data.table::fwrite(month_of_data, file = file.path(backup_dir, glue("{year}.csv.gz")))
