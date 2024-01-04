# OpenWeather Project
--------------------------------------------------------------------------------
Resume: In this project I did use the Open Wather API to take data and make the necessaries transformation. After the first process of extraction and transformation, i save at the data-lake, at AWS.

Steps:
- take data from openweather api
- create a secret group in AWS to protect my own token, this way I can use my token with no worries
- treat the JSON file and create a DataFrame
- export the DataFrame in .parquet format to my data lake at AWS
