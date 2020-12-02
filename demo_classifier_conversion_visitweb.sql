
###############################################################################
# CAS D'USAGE
# Construire un modèle pour savoir si un visiteur va convertir sa visite

# PARTIE 1 APPRENTISSAGE
# modele : régression logistyiuqe sur des données publiques provenant de Google Analytics

CREATE OR REPLACE MODEL `bqml_tutorial.sample_model`
OPTIONS(model_type='logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'
  ;


#To see the results of the model training, you can use the ML.TRAINING_INFO function, or you can view the statistics in the BigQuery web UI. This functionality is not currently available in the BigQuery Classic web UI. In this tutorial, you use the ML.TRAINING_INFO function.
#A machine learning algorithm builds a model by examining many examples and attempting to find a model that minimizes loss. This process is called empirical risk minimization.
#Loss is the penalty for a bad prediction — a number indicating how bad the model's prediction was on a single example. If the model's prediction is perfect, the loss is zero; otherwise, the loss is greater. The goal of training a model is to find a set of weights that have low loss, on average, across all examples.
#To see the model training statistics that were generated when you ran the CREATE MODEL query:

SELECT
  *
FROM
  ML.TRAINING_INFO(MODEL `bqml_tutorial.sample_model`)

 # The loss column represents the loss metric calculated after the given iteration on the training dataset.
 #Since you performed a logistic regression, this column is the log loss.
# The eval_loss column is the same loss metric calculated on the holdout dataset (data that is held back from training to validate the model).

###############################################################################
# PARTIE 2 EVALUATION
# After creating your model, you evaluate the performance of the classifier using the ML.EVALUATE function. You can also use the ML.ROC_CURVE function for logistic regression specific metrics.
#A classifier is one of a set of enumerated target values for a label. For example, in this tutorial you are using a binary classification model that detects transactions. The two classes are the values in the label column: 0 (no transactions) and not 1 (transaction made).
#To run the ML.EVALUATE query that evaluates the model:

SELECT
  *
FROM ML.EVALUATE(MODEL `bqml_tutorial.sample_model`, (
  SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(totals.pageviews, 0) AS pageviews
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))

###############################################################################
# PARTIE 3 PREDICTIONS
# Avec le modèle, effectuer des prédictions
# You use your model to predict the number of transactions made by website visitors from each country. And you use it to predict purchases per user.
# To run the query that uses the model to predict the number of transactions:


SELECT
  country,
  SUM(predicted_label) as total_predicted_purchases
FROM ML.PREDICT(MODEL `bqml_tutorial.sample_model`, (
  SELECT
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(geoNetwork.country, "") AS country
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
  GROUP BY country
  ORDER BY total_predicted_purchases DESC
  LIMIT 10

 # In the next example, you try to predict the number of transactions each website visitor will make. This query is identical to the previous query except for the GROUP BY clause. Here the GROUP BY clause — GROUP BY fullVisitorId — is used to group the results by visitor ID.
#To run the query that predicts purchases per user:

SELECT
  fullVisitorId,
  SUM(predicted_label) as total_predicted_purchases
FROM ML.PREDICT(MODEL `bqml_tutorial.sample_model`, (
  SELECT
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(geoNetwork.country, "") AS country,
    fullVisitorId
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
  GROUP BY fullVisitorId
  ORDER BY total_predicted_purchases DESC
  LIMIT 10