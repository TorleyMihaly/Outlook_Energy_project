# Outlook_Energy_project

1: To init DB, simply run the etl.py python file, it will pull the current weather data from openmeteo, and initialise it into an SQLite DB which lives in the data folder

2: The derived metric I used is wind chill, calculated inside the request function, although the formula is limited to only work below 10°C temperature and above 5km/h wind speed, it will still demonstrate a derived metric

3: To excecute the Select statements, simply go into the scripts folder and run them, the select_average will require an input as well for the number of hours in the future you want to look.
The Update statement is the same but two inputs are needed, one for the l0ocation ID and one for the name change
