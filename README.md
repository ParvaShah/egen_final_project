# GCP Data Pipeline

__In this Project we predict Apple’s Stock price 20 mins ahead of current time using Machine Learning and Google’s Cloud Architecture\.__

__We use Google’s Cloud Storage\, Cloud Function\, PUB/SUB\, BigQuery\, and Composer\.__

__Data for prediction is gathered from Twitter’s api for getting stock’s sentiment and IEX api for current stock price\.__

_Get data from_  _Api’s_  _in Docker and Push to Topic Where Cloud Function can retrieve the data as a subscriber and  store the files in Cloud Storage\._

<img src="img\EGEN FINAL PROJECT0.png" width=500px />

<img src="img\EGEN FINAL PROJECT1.jpg" width=500px />

<img src="img\EGEN FINAL PROJECT2.png" width=336px />

<img src="img\EGEN FINAL PROJECT3.png" width=500px />

<img src="img\EGEN FINAL PROJECT4.png" width=270px />

<img src="img\EGEN FINAL PROJECT5.png" width=335px />

_A Cloud Function gets triggered when a new file is detected in Cloud Storage which in turn triggers a DAG in Airflow\. DAG will be transforming the CSV data\, predicting prices using ML model and transferring the predictions to BigQuery table\._

<img src="img\EGEN FINAL PROJECT6.png" width=500px />

<img src="img\EGEN FINAL PROJECT7.png" width=500px />

<img src="img\EGEN FINAL PROJECT8.png" width=500px />

<img src="img\EGEN FINAL PROJECT9.png" width=440px />

_Fetch\, Transform and Predict_

_Insert data in BigQuery Table_

<img src="img\EGEN FINAL PROJECT10.png" width=256px />

_Create a REST API using Flask and expose the data via endpoint to a React App which will show the Predicted and Actual prices graph\._

<img src="img\EGEN FINAL PROJECT11.png" width=500px />

<img src="img\EGEN FINAL PROJECT12.png" width=438px />

<img src="img\EGEN FINAL PROJECT13.png" width=300px />

__INDEPENDENT PHASE__

_A DAG will be triggered every 15 days which will train model on the new data available and make it available for prediction task\._

<img src="img\EGEN FINAL PROJECT14.png" width=500px />

<img src="img\EGEN FINAL PROJECT15.png" width=500px />

<img src="img\EGEN FINAL PROJECT16.png" width=270px />

_Fetch and Transform Data_

<img src="img\EGEN FINAL PROJECT17.png" width=256px />

