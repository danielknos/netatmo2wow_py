import time
import datetime
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import urllib
import json
import os
from tqdm import tqdm
import requests
import platform

def get_credentials(location):
    """Get user credentials for specific location.

    Parameters:
    location -- The name of the location correpsonding to name in user_inputs.json
    
    Returns:
    Json child from user_inputs.json
    """
    with open('user_inputs.json') as f:
        data = json.load(f)
    no_of_locations = len(data['locations'])
    for i in range(0, no_of_locations):
        if data['locations'][i]['name'] == location:
            return(data['locations'][i])


def get_token(location):
    """Get authorization token from netatmo api for specific location.

    Parameters:
    location -- The name of the location correpsonding to name in user_inputs.json

    Returns:
    json:authorization token
    """
    credentials = get_credentials(location)
    url = 'https://api.netatmo.net/oauth2/token'
    data = [
        ('grant_type', 'password'),
        ('client_id', credentials['client_id']),
        ('client_secret', credentials['client_secret']),
        ('username', credentials['username']),
        ('password', credentials['password'])    
    ]
    response = requests.post(url, data=data)
    ret_value = response.json()
    return ret_value

def get_station_data(location, auth_token):
    """Get information about the data availability (parameters) for specific station

    Keyword arguments:
    location -- The name of the location correpsonding to name in user_inputs.json
    auth_token -- Authorization token as returned by get_token()

    Returns:
    json.response() information from api/getstationdata
    """
    credentials = get_credentials(location)
    data = [
        ('access_token', auth_token['access_token']),
        ('device_id', credentials['device_id'])
    ]

    url = 'https://api.netatmo.net/api/getstationsdata'
    response = requests.post(url, data=data)
    ret_data = response.json()
    return ret_data

def get_measurements(location, station_data, auth_token, scale_in, no_of_days = 1, savedata = False):
    """Get Measurements for specific station

    Keyword arguments:
    str:location -- The name of the location correpsonding to name in user_inputs.json
    json:station_data -- Station information as returned by get_station_data
    json:auth_token --  auth token as returned by get_token
    str:scale_in -- observation frequency as specifie by netatmo api (5min, 15min, 30min, 1hour...)
    float:no_of_days -- number of days back in time to fetch data
    Boolean:savedata -- save all downloaded data as data.csv

    Returns:
    pd:all_datasets pandas dataframe with columns
        -float:time - Linux time (UTC)
        -float:value - Observed value
        -str:netatmo_param - Weather parameter
        -datetime:time_utc - Actual time (UTC)
        -str:location - Location name
    """
    credentials = get_credentials(location)
    all_devices = station_data['body']['devices']
    all_modules = all_devices[0]['modules']
    end_time = time.time()
    start_time = end_time - no_of_days*24*3600
    all_datasets = pd.DataFrame(columns = ['time', 'value', 'netatmo_param'])

    for i in range(0, len(all_modules)):
        this_module = all_modules[i]
        this_module_id = this_module['_id']
        this_data_type = this_module['data_type']
        if 'Rain' in this_data_type:
            this_data_type.append('sum_rain') # No specific type exists for sum_rain so added manually if a rain gauge exists
        for j in range(0, len(this_data_type)):
            data={'access_token' : auth_token['access_token'],
                    'device_id': credentials['device_id'],
                    'scale' : scale_in,
                    'type': this_data_type[j],
                    'module' : this_module_id,
                    'date_begin': start_time,
                    'date_end': end_time,
                    'real_time' : 'false'}
            url = 'https://api.netatmo.net/api/getmeasure'
            detailed_url = (url + '?device_id=' + credentials['device_id'] + 
                            '&module_id=' + this_module_id + '&scale=' + scale_in + '&type=' + this_data_type[j] +
                            '&date_begin=' + str(start_time) +
                            '&date_end=' + str(end_time) +
                            '&access_token=' + auth_token['access_token'])
            response = requests.post(url, data=data)
            if response.status_code == 200:
                request = urllib.request.urlopen(detailed_url)
                if platform.system() == 'Windows':
                    this_values = json.load(request)['body']
                elif platform.system() == 'Linux':
                    this_values = json.loads(request.read().decode('utf-8'))['body']
                for k in range(0,len(this_values)):
                    this_beg_time = this_values[k]['beg_time']
                    this_step_time = this_values[k]['step_time']
                    this_value = np.array(this_values[k]['value']).flatten()
                    no_of_values = len(this_value)
                    this_time = np.linspace(this_beg_time, this_beg_time + (no_of_values - 1) * this_step_time, no_of_values)
                    dataset = pd.DataFrame({'time': this_time, 'value': this_value, 'netatmo_param' : this_data_type[j]}, columns=['time', 'value', 'netatmo_param'])
                    all_datasets = all_datasets.append(dataset)
    all_datasets['time_utc'] = pd.to_datetime(all_datasets['time'], unit='s') 
    all_datasets['location'] = location
    all_datasets = all_datasets[['location', 'time', 'time_utc', 'netatmo_param', 'value']]
    if savedata:
        all_datasets.to_csv('data.csv', index = False)
    return(all_datasets)


def upload_measurements(location, measurements, update_freq, timeshift_for_zero):
    """Upload measurements to wow website

    Keyword arguments:
    str:location -- The name of the location correpsonding to name in user_inputs.json
    pd:measurements -- Measurements as returned by get_measurements
    str:update_freq -- Update frequency to wow, same structure as datetime.round() (10min, 20min...)
    int:timeshift_for_zero -- Number of hours shift for setting accumulated values to 0 (UTC = 0, CET = 1...)

    Returns:
    No returned values

    Outputs:
    csv:uploaded_data.csv -- All uploaded data saved as csv
    csv:failed_data.csv -- All data failed to upload as csv
    csv:upload_log.csv -- Saved timestamp of last upload, used to avoid upload of same data twice
    """
    credentials = get_credentials(location)
    data={'siteAuthenticationKey' : credentials['authentication_key'],
        'siteid': credentials['site_id'],
        'softwaretype' : 'netatmo2wow_py'
    }
    url = 'http://wow.metoffice.gov.uk/automaticreading?'    
    if os.path.isfile('upload_log.csv'):
        # Reading the old log if it exists
        log_exist = True
        upload_log_last = pd.read_csv('upload_log.csv')
        this_log = upload_log_last[upload_log_last.location == location]
        
        # Finds the last accumulated rainfall if it exists
        if 'sum_rain' in this_log.columns:
            current_max_accum_rainfall = this_log[['time', 'sum_rain']]
            current_max_accum_rainfall['date'] = pd.to_datetime(current_max_accum_rainfall['time'])
            current_max_accum_rainfall['date'] = current_max_accum_rainfall['date'].dt.date
            current_max_accum_rainfall = current_max_accum_rainfall[['date','time', 'sum_rain']]
            last_time_per_date = current_max_accum_rainfall.groupby('date').time.max().reset_index()
            max_accum_rainfall = last_time_per_date.merge(current_max_accum_rainfall)

        else:
            max_accum_rainfall = pd.DataFrame({}, columns=['date', 'sum_rain'], index = [0])
        # Filters out the observaitons that are valid for times after the last upload
        measurements = measurements[measurements.time_utc > max(this_log['time'])]
        measurements.to_csv('uploading_measurements.csv')
    else:
        upload_log_last = pd.DataFrame({}, columns=['location', 'time','Rain','Temperature', 'Humidity', 'Wind', 'sum_rain'], index = [0])
        max_accum_rainfall = pd.DataFrame({}, columns=['date', 'sum_rain'], index = [0])
        log_exist = False
    
    measurements['time_utc_rounded'] = measurements['time_utc'].dt.round(update_freq)
    measurements = measurements[['time_utc_rounded', 'location', 'netatmo_param', 'value']]

    all_param = measurements.netatmo_param.unique()
    first_param = 1
    for i in range(0, len(all_param)):
        if all_param[i] in ['Temperature', 'Humidity', 'Wind']:
            df = pd.pivot_table(measurements[measurements.netatmo_param == all_param[i]], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.mean)    
        elif all_param[i] in ['Rain', 'sum_rain']:
            df = pd.pivot_table(measurements[measurements.netatmo_param == all_param[i]], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.sum)    
        if first_param:
            df_all = df
            first_param = 0
        else: 
            df_all = df_all.merge(df, how = 'left', on = 'time_utc_rounded')
        
    
    measurements = df_all
    measurements.columns = measurements.columns.droplevel(0) #remove amount
    measurements.columns.name = None               #remove categories
    measurements = measurements.reset_index()
    measurements = measurements.sort_values(by=['time_utc_rounded'])     

    uploaded_data = pd.DataFrame({}, columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity', 'Wind'], index = [0])
    failed_data = pd.DataFrame({}, columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity', 'Wind'], index = [0])
    current_date = ''
    for i in tqdm(range(0, len(measurements))):
        if 'Rain' in measurements.columns:
            this_date = (measurements['time_utc_rounded'].iloc[i] + timedelta(hours = timeshift_for_zero)).date()
            this_max_accum_rainfall = max_accum_rainfall[max_accum_rainfall.date == this_date]
            if this_date != current_date:
                if(this_max_accum_rainfall.shape[0] > 0):
                    rain_prior = this_max_accum_rainfall['sum_rain'].max()
                    if np.isnan(rain_prior):
                        rain_prior = 0
                    daily_accum = rain_prior + measurements['sum_rain'].iloc[i]
                    # All measurements prior to last upload has already been removed so only need to look for the date here, not time
                else:
                    daily_accum = 0
                current_date = this_date
            else:
                daily_accum = daily_accum + measurements['sum_rain'].iloc[i]
        
        data['dateutc'] = str(measurements['time_utc_rounded'].iloc[i]).replace(':', '%3A').replace(' ', '+')
        this_data = pd.DataFrame({'location':location,
                                    'time': measurements['time_utc_rounded'].iloc[i]}, index = [0])
        if 'Rain' in measurements.columns:
            data['rainin'] = str(measurements['Rain'].iloc[i] * 0.039370079) # Converting mm to inches
            data['dailyrainin'] = str(daily_accum * 0.039370079)
            this_data['Rain'] = measurements['Rain'].iloc[i],
            this_data['sum_rain'] = float(daily_accum)
        if 'Temperature' in measurements.columns:
            data['tempf'] = str(measurements['Temperature'].iloc[i] *1.8 + 32) # Converting Celsius to Fahrenheit
            this_data['Temperature'] = measurements['Temperature'].iloc[i]
        if 'Humidity' in measurements.columns:
            data['humidity'] = str(measurements['Humidity'].iloc[i]) 
            this_data['Humidity'] = measurements['Humidity'].iloc[i]
        response = requests.post(url, data=data)
        if response.status_code == 200:
        # if 1:
            uploaded_data = uploaded_data.append(this_data)
        else:
            failed_data = failed_data.append(this_data)
    
    uploaded_data.to_csv('uploaded_data.csv')
    failed_data.to_csv('failed_data.csv')
    tmp = uploaded_data
    tmp = tmp.groupby(tmp['time'].dt.date).last().reset_index(drop=True)
    upload_log_new = upload_log_last.append(tmp)    
    upload_log_new['time'] = pd.to_datetime(upload_log_new['time'])
    upload_log_new.to_csv('upload_log.csv', index = False)


