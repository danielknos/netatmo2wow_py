import time
import os
import sys  
from helper_functions import get_token, get_credentials, get_station_data, get_measurements, upload_measurements

def main(location):
    auth_token = get_token(location)
    station_data = get_station_data(location, auth_token)
    measurements = get_measurements(location, station_data, auth_token, '5min', no_of_days = 1, savedata = False)
    upload_measurements(location, measurements, update_freq = '10min', timeshift_for_zero = 1)

if __name__ == "__main__":
    # location = sys.argv[1]
    location = 'falhagen'
    main(location)