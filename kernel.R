library(readr)
library(magrittr) # For %<>%
library(dplyr)
library(lubridate)
library(ggplot2)
library(scales) # For percent()
library(jsonlite)


# Loading data and preprocessing ------------------------------------------

file_path <- c("~/Datasets/ga-customer-revenue-prediction")

tr <- read_csv(file.path(file_path, "train.csv"))
te <- read_csv(file.path(file_path, "test.csv"))

# Convert BigQuery columns and response variable
# https://support.google.com/analytics/answer/3437719?hl=en
tr %<>%
  bind_cols(fromJSON(paste("[", paste(.$device, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$geoNetwork, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$totals, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$trafficSource, collapse = ","), "]"),
    flatten = TRUE)) %>%
  select(-device, -geoNetwork, -totals, -trafficSource) %>% 
  mutate(transactionRevenue = ifelse(is.na(transactionRevenue), 0,
    as.numeric(transactionRevenue)))

te %<>% 
  bind_cols(fromJSON(paste("[", paste(.$device, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$geoNetwork, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$totals, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$trafficSource, collapse = ","), "]"),
    flatten = TRUE)) %>%
  select(-device, -geoNetwork, -totals, -trafficSource) %>% 
  mutate(transactionRevenue = -1)

# Match columns between train and test set
setdiff(colnames(tr), colnames(te))
tr$campaignCode <- NULL # Not present in test data

# Bind test and train to easier processing and to ensure same factor decoding
# (these can be separated by response: -1 in test)
fu <- bind_rows(tr, te) %>% 
  mutate(date = as.Date(as.character(date), format = "%Y%m%d"),
    visitStartTime = as.POSIXct(visitStartTime, tz = "UTC",
      origin = "1970-01-01")) %>% 
  mutate(year = year(date), month = month(date), day = day(date),
    weekday = wday(date), yearday = yday(date),
    visitStartYear = year(visitStartTime),
    visitStartMonth = month(visitStartTime),
    visitStartDay = day(visitStartTime),
    visitStartWeekday = wday(visitStartTime),
    visitStartYearday = yday(visitStartTime),
    visitStartHour = hour(visitStartTime),
    visitStartMinute = minute(visitStartTime)) %>%
    select(-date, -visitStartTime)

rm(tr, te)

# Convert numeric columns
cols_num <- c("visits", "hits", "pageviews", "bounces", "newVisits",
  "adwordsClickInfo.page")

fu %<>% mutate_at(cols_num, funs(as.numeric(.)))

# Missing Data
cols_na <- colSums(is.na(fu)) %>%
{data_frame(variable = names(.), nan = .)} %>%
  filter(nan > 0) %>%
  mutate(nanp = nan / nrow(fu))

ggplot(cols_na, aes(x = reorder(variable, nan), y = nanp)) +
  geom_bar(stat = "identity", color = "black", fill = "dodgerblue") +
  labs(y = "Missing Percentage", x = "Variable") +
  geom_label(aes(x = reorder(variable, nan), y = nanp, label = percent(nanp))) +
  coord_flip()

# Numeric columns that that have missing values
intersect(cols_na$variable, cols_num)

fu$pageviews[is.na(fu$pageviews)] <- 0
fu$bounces[is.na(fu$bounces)] <- 0
fu$adwordsClickInfo.page[is.na(fu$adwordsClickInfo.page)] <- 0
fu$newVisits[is.na(fu$newVisits)] <- 0

# Store details about missing data, this is used later:
cols_na <- colSums(is.na(fu)) %>%
{data_frame(variable = names(.), nan = .)} %>%
  filter(nan > 0) %>%
  mutate(nanp = nan / nrow(fu))


# Exploring data ----------------------------------------------------------

# Taking a look at the categorical variables
colnames(fu)[sapply(fu, class) == 'character']

# How many levels they have?
freqs <- mapply(table, fu[, sapply(fu, class) == 'character'])
sapply(freqs, length)


# Baseline Model ----------------------------------------------------------

# Preparing data for modeling

# Id columns that need to stay as chr
ids <- c("fullVisitorId", "sessionId")

# Simply drop variables with missing data, convert categorical variables
fu %<>%
  select(-one_of(cols_na$variable)) %>%
  mutate_at(colnames(select(., -one_of(ids)))[sapply(., class) == "character"], funs(factor(.))) %>%
  mutate_at(colnames(.)[sapply(., class) == "factor"], funs(as.numeric(.))) %>%
  mutate(PredictedLogRevenue = ifelse(transactionRevenue > 0,
    log(transactionRevenue), transactionRevenue)) %>%
  select(-transactionRevenue)

tr <- fu %>% 
  filter(PredictedLogRevenue != -1)

te <- fu %>% 
  filter(PredictedLogRevenue == -1)

# Sanity check
message(paste("anyNA tr:", anyNA(tr)))
message(paste("anyNA te:", anyNA(te)))
