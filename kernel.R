library(magrittr) # For %<>%
library(dplyr)
library(lubridate)
library(ggplot2)
library(scales) # For percent()
library(jsonlite)
library(stringr)
library(purrr)
library(lightgbm)
library(rBayesianOptimization)
library(foreach)
library(ModelMetrics)
library(readr)

# MANY CATEGORICAL VARIABLES NOT CLEANED (Done: campaign, browser, source)
# hits column not used in this script
# Loading data and preprocessing ------------------------------------------

file_path <- c("~/Datasets/ga-customer-revenue-prediction")

tr <- read_csv(file.path(file_path, "train_v2.csv"), #n_max = load_max,
    col_types = cols(
      socialEngagementType = readr::col_skip(), # Always the same
      hits = readr::col_skip(), # skip at this point
      customDimensions = readr::col_factor(NULL), # Read as factor to save mem
      channelGrouping = readr::col_factor(NULL) # Read as factor to save mem
      ))

te <- read_csv(file.path(file_path, "test_v2.csv"), #n_max = load_max,
  col_types = cols(
    socialEngagementType = readr::col_skip(),
    hits = readr::col_skip(), # skip at this point
    customDimensions = readr::col_factor(levels = levels(tr$customDimensions)),
    channelGrouping = readr::col_factor(levels = levels(tr$channelGrouping))
    ))

# If not skipping hits, use these lines and hits.R-script:
# tr_hits <- tr %>% select(fullVisitorId, visitStartTime, hits)
# te_hits <- te %>% select(fullVisitorId, visitStartTime, hits)
# 
# tr %<>% select(-hits)
# te %<>% select(-hits)

# Convert BigQuery columns and response variable
# https://support.google.com/analytics/answer/3437719?hl=en
tr %<>%
  bind_cols(fromJSON(paste("[", paste(.$device, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$geoNetwork, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$totals, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$trafficSource, collapse = ","), "]"),
    flatten = TRUE)) %>%
  dplyr::select(-device, -geoNetwork, -totals, -trafficSource) %>% 
  mutate(transactionRevenue = ifelse(is.na(transactionRevenue), 0,
    as.numeric(transactionRevenue)))

te %<>% 
  bind_cols(fromJSON(paste("[", paste(.$device, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$geoNetwork, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$totals, collapse = ","), "]"))) %>%
  bind_cols(fromJSON(paste("[", paste(.$trafficSource, collapse = ","), "]"),
    flatten = TRUE)) %>%
  dplyr::select(-device, -geoNetwork, -totals, -trafficSource) %>% 
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
    dplyr::select(-date, -visitStartTime)

rm(tr, te)

# Convert numeric columns
numerical_variables <- c("visits", "hits", "pageviews", "bounces", "newVisits",
  "adwordsClickInfo.page")

fu %<>% mutate_at(numerical_variables, funs(as.numeric(.)))

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
intersect(cols_na$variable, numerical_variables)

fu$pageviews[is.na(fu$pageviews)] <- 0
fu$bounces[is.na(fu$bounces)] <- 0
fu$adwordsClickInfo.page[is.na(fu$adwordsClickInfo.page)] <- 0
fu$newVisits[is.na(fu$newVisits)] <- 0

# Store details about missing data, this is used later:
cols_na <- colSums(is.na(fu)) %>%
{data_frame(variable = names(.), nan = .)} %>%
  filter(nan > 0) %>%
  mutate(nanp = nan / nrow(fu))

# Id columns that need to stay as chr
ids <- c("fullVisitorId", "sessionId")

# Convert response variable to log
fu %<>%
  mutate(PredictedLogRevenue = ifelse(transactionRevenue > 0,
    log1p(transactionRevenue), transactionRevenue)) %>%
  dplyr::select(-transactionRevenue)

# Clean source, browser and campaign column a bit
fu %<>% mutate(source = gsub(":.*", "", source)) %>% 
  mutate(source = gsub("^google.*", "google", source)) %>% 
  mutate(source = gsub("^bing.*", "bing", source)) %>% 
  mutate(source = gsub("^baidu.*", "baidu", source)) %>% 
  mutate(source = gsub("^adwords.*", "adwords", source)) %>% 
  mutate(source = gsub("^blackboard.*", "blackboard", source)) %>% 
  mutate(source = gsub("^businessinsider.*", "businessinsider", source)) %>% 
  mutate(source = gsub("^gdeals.*", "gdeals", source)) %>% 
  mutate(source = gsub("^images.google.*", "images.google", source)) %>%
  mutate(source = gsub("^yahoo.*", "yahoo", source)) %>%
  mutate(source = gsub("^search.*", "search", source)) %>%
  mutate(source = gsub("^mail.*", "mail", source)) %>%
  mutate(source = gsub("^msn.*", "msn", source)) %>%
  mutate(source = gsub("^m\\.", "", source))

fu %<>% mutate(browser = gsub("^0$", "(not set)", browser)) %>% 
  mutate(browser = gsub("^Nokia.*", "Nokia", browser)) %>% 
  mutate(browser = gsub("^LYF.*", "LYF", browser)) %>% 
  mutate(browser = gsub("^Mozilla.*", "Mozilla", browser)) %>% 
  mutate(browser = gsub("^Android.*", "Android", browser)) %>% 
  mutate(browser = gsub("^Safari.*", "Safari", browser)) %>% 
  mutate(browser = gsub("^Opera.*", "Opera", browser))

# Campaign:
fu %<>% mutate(campaign = gsub(".*Accessories March 17.*",
  "Accessories March 17", campaign)) %>% 
  mutate(campaign = gsub(".*google\\+redesign/bags.*",
    "Google Redesign Bags", campaign)) %>% 
  mutate(campaign = gsub(".*google\\+redesign/drinkware.*",
    "Google Redesign Drinkware", campaign)) %>% 
  mutate(campaign = gsub(".*google\\+redesign/electronics.*",
    "Google Redesign Electronics", campaign)) %>% 
  mutate(campaign = gsub(".*google\\+redesign/office.*",
    "Google Redesign Office", campaign))

# There are also some other columns that are the same value, drop these:
drop_us <- fu %>%
  summarise_all(funs(n_distinct(.))) %>%
  select_if(. == 1) %>%
  colnames()

fu %<>% dplyr::select(-one_of(c(drop_us, "visitId", "totalTransactionRevenue", "transactions")))

fu %<>% mutate(timeOnSite = as.numeric(timeOnSite),
  sessionQualityDim = as.numeric(sessionQualityDim),
  isTrueDirect = as.numeric(isTrueDirect),
  adwordsClickInfo.isVideoAd = as.numeric(adwordsClickInfo.isVideoAd),
  isMobile = as.numeric(isMobile))

# Deal with missing values: 
# chr columns: convert NA to default missing, add binary column to indicate this
# was not present:
# keyword: (not provided)
fu$keyword_isBAD <- 0
fu[is.na(fu$keyword), ]$keyword_isBAD <- 1
fu[is.na(fu$keyword), ]$keyword <- "(not provided)"

# referralPath
fu$referralPath_isBAD <- 0
fu[is.na(fu$referralPath), ]$referralPath_isBAD <- 1
fu[is.na(fu$referralPath), ]$referralPath <- "NA"

# adContent
fu$adContent_isBAD <- 0
fu[is.na(fu$adContent), ]$adContent_isBAD <- 1
fu[is.na(fu$adContent), ]$adContent <- "NA"

# adwordsClickInfo.gclId
fu$adwordsClickInfo.gclId_isBAD <- 0
fu[is.na(fu$adwordsClickInfo.gclId), ]$adwordsClickInfo.gclId_isBAD <- 1
fu[is.na(fu$adwordsClickInfo.gclId), ]$adwordsClickInfo.gclId <- "NA"

# adwordsClickInfo.adNetworkType
fu$adwordsClickInfo.adNetworkType_isBAD <- 0
fu[is.na(fu$adwordsClickInfo.adNetworkType), ]$adwordsClickInfo.adNetworkType_isBAD <- 1
fu[is.na(fu$adwordsClickInfo.adNetworkType), ]$adwordsClickInfo.adNetworkType <- "NA"

# adwordsClickInfo.slot
fu$adwordsClickInfo.slot_isBAD <- 0
fu[is.na(fu$adwordsClickInfo.slot), ]$adwordsClickInfo.slot_isBAD <- 1
fu[is.na(fu$adwordsClickInfo.slot), ]$adwordsClickInfo.slot <- "NA"

# num columns: NA --> 0, add binary column to indicate missing value

# sessionQualityDim
fu$sessionQualityDim_isBAD <- 0
fu[is.na(fu$sessionQualityDim), ]$sessionQualityDim_isBAD <- 1
fu[is.na(fu$sessionQualityDim), ]$sessionQualityDim <- 0
# timeOnSite
fu$timeOnSite_isBAD <- 0
fu[is.na(fu$timeOnSite), ]$timeOnSite_isBAD <- 1
fu[is.na(fu$timeOnSite), ]$timeOnSite <- 0

# Logical columns: -1 and convert to numeric
fu[is.na(fu$isTrueDirect), ]$isTrueDirect <- 0
fu[is.na(fu$adwordsClickInfo.isVideoAd), ]$adwordsClickInfo.isVideoAd <- 1 # Current levels 0 / NA

# Split back to training and test data
tr <- fu %>% 
  filter(PredictedLogRevenue != -1)

te <- fu %>% 
  filter(PredictedLogRevenue == -1)

rm(fu)

# Trim down sources, browser, campaign column as it now has too many levels for
# one hot-encoding
source_categories <- tr %>% 
  group_by(source) %>%
  summarize(total = n()) %>%
  arrange(desc(total)) %>% 
  top_n(20, total) %>% 
  .$source

browser_categories <- tr %>% 
  group_by(browser) %>%
  summarize(total = n()) %>%
  arrange(desc(total)) %>% 
  top_n(10, total) %>% 
  .$browser

tr %<>% mutate(
  source = ifelse(source %in% source_categories, source, "other"),
  browser = ifelse(browser %in% browser_categories, browser, "other"),
  campaign = ifelse(campaign %in% c("All Products", "AW - Electronics",
    "Data Share"), "other", campaign))

te %<>% mutate(
  source = ifelse(source %in% source_categories, source, "other"),
  browser = ifelse(browser %in% browser_categories, browser, "other"),
  campaign = ifelse(campaign %in% c("All Products", "AW - Electronics",
    "Data Share"), "other", campaign))

rm(source_categories, browser_categories)

# Convert all character columns to numeric
fu <- bind_rows(tr, te)

fu %<>%
  select(-one_of(ids)) %>%
  mutate_at(colnames(.)[sapply(., class) == "character"],
  funs(factor(.))) %>% mutate_at(colnames(.)[sapply(., class) == "factor"], funs(as.numeric(.))) %>%
  mutate(fullVisitorId = fu$fullVisitorId)

# Split back to training and test data
tr <- fu %>% 
  filter(PredictedLogRevenue != -1)

te <- fu %>% 
  filter(PredictedLogRevenue == -1)

rm(fu)

# LightGBM Model ----------------------------------------------------------
# LightGBM offers good accuracy with integer-encoded categorical features.
# LightGBM applies Fisher (1958) to find the optimal split over categories as
# described here. This often performs better than one-hot encoding. 

fold_me <- ifelse(tr$PredictedLogRevenue > 0, 1, 0)
cv_folds <- caret::createFolds(fold_me, k = 5, list = TRUE)
rm(fold_me)

numerical_features <- c("visitNumber", "visits", "hits", "pageviews", "bounces",
  "newVisits", "adwordsClickInfo.page", "newVisits", "visitStartYearday",
  "visitStartHour", "visitStartMinute", "PredictedLogRevenue")

categorical_features <- setdiff(colnames(tr), c(numerical_features, ids))

lgb.unloader(wipe = TRUE) # Just incase (if lgb.Dataset in memory might crash R)

dataset <- tr

tr_lgb <- lgb.Dataset(data = as.matrix(
  dplyr::select(dataset, -fullVisitorId, -PredictedLogRevenue)),
  label = dataset$PredictedLogRevenue,
  categorical_feature = categorical_features)

# Bayesian Optimization ---------------------------------------------------
# Helper function to predict with cv model
lgb_cv_predict <- function(lgb_cv, data, folds) {
  num_iteration <- lgb_cv$best_iter
  cv_pred_mat <- foreach::foreach(i = seq_along(lgb_cv$boosters), .combine = "rbind") %do% {
    lgb_tree <- lgb_cv$boosters[[i]][[1]]
    predict(lgb_tree, 
      data[folds[[i]],], 
      num_iteration = num_iteration, 
      rawscore = FALSE, predleaf = FALSE, header = FALSE, reshape = TRUE)
  }
  as.double(cv_pred_mat)[order(unlist(folds))]
}

lgbm_cv_bayes <- function(max_depth, num_leaves, min_data_in_leaf, 
  min_sum_hessian_in_leaf, feature_fraction, bagging_fraction, bagging_freq, 
  lambda_l1, lambda_l2, min_gain_to_split) {
  lgb_params <- list(
    objective = "regression",
    metric = "rmse",
    max_depth = max_depth,
    num_leaves = num_leaves,
    min_data_in_leaf = min_data_in_leaf,
    min_sum_hessian_in_leaf = min_sum_hessian_in_leaf,
    feature_fraction = feature_fraction,
    bagging_fraction = bagging_fraction,
    bagging_freq = bagging_freq,
    lambda_l1 = lambda_l1,
    lambda_l2 = lambda_l2,
    min_gain_to_split = min_gain_to_split)
  
  cv_model <- lgb.cv(
    params = lgb_params,
    data = tr_lgb,
    folds = cv_folds,
    nrounds = 5000,
    device_type = "cpu",
    early_stopping_rounds = 50,
    record = TRUE,
    eval_freq = 10)
    
    p <- lgb_cv_predict(
      cv_model,
      data = as.matrix(select(tr, -PredictedLogRevenue, -fullVisitorId)),
      folds = cv_folds)
  
    list(Score = cv_model$best_score,
      Pred = p)
}

my_tune <- BayesianOptimization(lgbm_cv_bayes,
  bounds = list(
    max_depth = c(5L, 80L),
    num_leaves = c(31L, 256L),
    min_data_in_leaf = c(1L, 30L),
    min_sum_hessian_in_leaf = c(0.05, 0.2),
    feature_fraction = c(0.8, 0.95),
    bagging_fraction = c(0.4, 0.6),
    bagging_freq = c(0L, 4L),
    lambda_l1 = c(0, 0.4),
    lambda_l2 = c(0, 0.4),
    min_gain_to_split = c(0, 0.4)),
  init_grid_dt = NULL, init_points = 10, n_iter = 20, # random starts, iterations
  acq = "ucb", kappa = 2.576, eps = 0.0,
  verbose = TRUE)

# Variable pairs for possible visualization:
# max_depth, num_leaves
# min_data_in_leaf, min_sum_hessian_in_leaf
# feature_fraction, bagging_fraction
# bagging_freq, min_gain_to_split
# lambda_l1, lambda_l2
#my_tune$History

# Predictions -------------------------------------------------------------
lgb_params <- list(
  objective = "regression",
  metric = "rmse",
  max_depth = my_tune$Best_Par["max_depth"],
  num_leaves = my_tune$Best_Par["num_leaves"],
  min_data_in_leaf = my_tune$Best_Par["min_data_in_leaf"],
  min_sum_hessian_in_leaf = my_tune$Best_Par["min_sum_hessian_in_leaf"],
  feature_fraction = my_tune$Best_Par["feature_fraction"],
  bagging_fraction = my_tune$Best_Par["bagging_fraction"],
  bagging_freq = my_tune$Best_Par["bagging_freq"],
  lambda_l1 = my_tune$Best_Par["lambda_l1"],
  lambda_l2 = my_tune$Best_Par["lambda_l2"],
  min_gain_to_split = my_tune$Best_Par["min_gain_to_split"])

# Running cv_model to get nrounds for the learning rate
cv_model <- lgb.cv(
  params = lgb_params,
  data = tr_lgb,
  nfold = 5, 
  nrounds = 5000, 
  device_type = "cpu",
  early_stopping_rounds = 50,
  record = FALSE,
  eval_freq = 10)

# Model to predict with
lgb_model <- lgb.train(
  params = lgb_params,
  data = tr_lgb,
  record = FALSE,
  nrounds = cv_model$best_iter,
  two_round_loading = FALSE,
  device_type = "cpu",
  eval_freq = 20#,
  #gpu_use_dp = TRUE
)

# Predict and submission file
preds <- te %>%
  select(fullVisitorId) %>%
  mutate(PredictedLogRevenue = predict(lgb_model,
    as.matrix(select(te, -fullVisitorId, -PredictedLogRevenue)))) %>%

  mutate(PredictedLogRevenue = ifelse(PredictedLogRevenue > 0,
    PredictedLogRevenue, 0)) %>% # Remove negative predictions
  group_by(fullVisitorId) %>%
  summarize(PredictedLogRevenue = sum(PredictedLogRevenue)) %>% 
  # In training there are no values under 9:
  mutate(PredictedLogRevenue = ifelse(PredictedLogRevenue > 1,
    PredictedLogRevenue, 0))

submission <- read_csv(file.path(file_path, "sample_submission_v2.csv")) %>%
  select(fullVisitorId) %>%
  left_join(preds, by = "fullVisitorId")

submission %>% write_csv("LightGBM_LogRevenue.csv")
