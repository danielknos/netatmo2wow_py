# netatmo2wow_py

Code to download measurements from Netatmo and upload them to the UK Met ofiice WOW website
- http://wow.metoffice.gov.uk/

The code currently handles the following parameters

    - temperature
    - humidity
    - pressure
    - accumulated rainfall
    - rainfall intensity
    - wind direction
    - wind speed


For the code to work the user needs to specify the station specific parameters in the json file user_inputs.json. These parameters can be found at the netatmo dev website and at the UK met office wow website

- name - Name of the station, this is specified by the user to keep the data separate if there are many stations
- device_id - The adress of the netatmo main device, can be found at the Netatmo dev website and is unique by station
- client_id - Netatmo specific client_id, ususally the same for all stations if the user har more than one
- client_secret - Netatmo secret, same for all stations
- username - Netatmo username
- password - Netatmo password
- authentication_key - Authentication key at the wow website
- site_id - Site ID for the WOW station

	
The code sequentially calls 4 functions
-get_token(location) - Gets authorization token for a station
-station_data(location, token) - ets information about the station, mainly which parameters that exist
-get_measurements(location, station_data, auth_token, frequency, no_of_days = 1, savedata = True) - Gets data for the station at location. Frequency is how often observations are read, specified by the Netatmo API. Available ones are 5min, 30min, 1hour etc.
no_of_days specifies how many days back the code goes to read data. If the upload is done ones a day then there's no point going further back than 1 day because thw upload to wow only uploads the new numbers. Set savedata to TRUE to save all the loaded data in a csv file
-upload_measurements(location, measurements, update_freq = '10min', timeshift_for_zero = 1)
Uploads the data to wow. update_freq specifies the frequency of uploads. The code rounds all the data to closes update_freq time unit and either averages it (temp, humidity, wind) or sums it (sum_rain) depending on the parameter	
- timeshift_for_zero. This specifies the number of hours from UTC the station is located. All times are in UTC but for accumulated rain it's set to 0 every new day and to make this in local time this can be shifted, for example to make it reset at midnight in CET instread of UTC use timeshift_for_zero = 1