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
    with open('user_inputs.json') as f:
        data = json.load(f)
    no_of_locations = len(data['locations'])
    for i in range(0, no_of_locations):
        if data['locations'][i]['name'] == location:
            return(data['locations'][i])


def get_token(location):
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
    credentials = get_credentials(location)
    all_devices = station_data['body']['devices']
    all_modules = all_devices[0]['modules']
    end_time = time.time()
    start_time = end_time - no_of_days*24*3600
    # start_time = datetime.date.today() - datetime.timedelta(1)
    # start_time = time.mktime(start_time.timetuple())
    all_datasets = pd.DataFrame(columns = ['time', 'value', 'netatmo_param'])

    for i in range(0, len(all_modules)):
        this_module = all_modules[i]
        this_module_id = this_module['_id']
        this_data_type = this_module['data_type']
        this_data_type.append('sum_rain')
        for j in range(0, len(this_data_type)):
            print(this_data_type[j])
            print(this_module_id)
            
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
            print(detailed_url)
            response = requests.post(url, data=data)
            if response.status_code == 200:
                request = urllib.request.urlopen(detailed_url)
                if platform.system() == 'Windows':
                    this_values = json.load(request)['body']
                elif platform.system() == 'Linux':
                    this_values = json.loads(request.read().decode('utf-8'))['body']
                print('Getting ' + this_data_type[j])
                for k in range(0,len(this_values)):
                    this_beg_time = this_values[k]['beg_time']
                    this_step_time = this_values[k]['step_time']
                    this_value = np.array(this_values[k]['value']).flatten()
                    no_of_values = len(this_value)
                    this_time = np.linspace(this_beg_time, this_beg_time + (no_of_values - 1) * this_step_time, no_of_values)
                    dataset = pd.DataFrame({'time': this_time, 'value': this_value, 'netatmo_param' : this_data_type[j]}, columns=['time', 'value', 'netatmo_param'])
                    all_datasets = all_datasets.append(dataset)
    all_datasets['time_utc'] = pd.to_datetime(all_datasets['time'], unit='s') # + pd.Timedelta('01:00:00')
    all_datasets['location'] = location
    if savedata:
        all_datasets.to_csv('data.csv')
    return(all_datasets)


def upload_measurements(location, measurements, update_freq, timeshift_for_zero):
    credentials = get_credentials(location)
    data={'siteAuthenticationKey' : credentials['authentication_key'],
        'siteid': credentials['site_id'],
        'softwaretype' : 'weathersoftware1.0'
    }
    url = 'http://wow.metoffice.gov.uk/automaticreading?'    
    if os.path.isfile('upload_log.csv'):
        log_exist = True
        upload_log_last = pd.read_csv('upload_log.csv')
        this_log = upload_log_last[upload_log_last.location == location]
        current_max_accum_rainfall = this_log[['time', 'sum_rain']]
        current_max_accum_rainfall['date'] = pd.to_datetime(current_max_accum_rainfall['time'])
        current_max_accum_rainfall['date'] = current_max_accum_rainfall['date'].dt.date
        current_max_accum_rainfall = current_max_accum_rainfall[['date','time', 'sum_rain']]
        last_time_per_date = current_max_accum_rainfall.groupby('date').time.max().reset_index()
        max_accum_rainfall = last_time_per_date.merge(current_max_accum_rainfall)
        # max_accum_rainfall = current_max_accum_rainfall.loc[current_max_accum_rainfall.groupby(["date"])["sum_rain"].idxmax()]  
        measurements = measurements[measurements.time_utc > max(this_log['time'])]
        measurements.to_csv('uploading_measurements.csv')
    else:
        upload_log_last = pd.DataFrame({}, columns=['location', 'time','Rain','Temperature', 'Humidity', 'Wind', 'sum_rain'], index = [0])
        max_accum_rainfall = pd.DataFrame({}, columns=['date', 'sum_rain'], index = [0])
        log_exist = False
    
    measurements['time_utc_rounded'] = measurements['time_utc'].dt.round(update_freq)
    measurements = measurements[['time_utc_rounded', 'location', 'netatmo_param', 'value']]
    df1 = pd.pivot_table(measurements[measurements.netatmo_param == 'Rain'], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.sum)
    df2 = pd.pivot_table(measurements[measurements.netatmo_param == 'sum_rain'], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.sum)
    df3 = pd.pivot_table(measurements[measurements.netatmo_param == 'Temperature'], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.mean)
    df4 = pd.pivot_table(measurements[measurements.netatmo_param == 'Humidity'], values=['value'], index=['time_utc_rounded'], columns = 'netatmo_param', aggfunc=np.mean)
    
    measurements = pd.concat((df1, df2, df3, df4), axis=1)
    measurements.columns = measurements.columns.droplevel(0) #remove amount
    measurements.columns.name = None               #remove categories
    measurements = measurements.reset_index()
    measurements = measurements.sort_values(by=['time_utc_rounded'])     

    uploaded_data = pd.DataFrame({}, columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity', 'Wind'], index = [0])
    failed_data = pd.DataFrame({}, columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity', 'Wind'], index = [0])
    current_date = ''
    for i in tqdm(range(0, len(measurements))):
        this_date = (measurements['time_utc_rounded'].iloc[i] + timedelta(hours = timeshift_for_zero)).date()
        this_max_accum_rainfall = max_accum_rainfall[max_accum_rainfall.date == this_date]
        if this_date != current_date:
            if(this_max_accum_rainfall.shape[0] > 0):
                daily_accum = this_max_accum_rainfall['sum_rain'].max() + measurements['sum_rain'].iloc[i]
                # All measurements prior to last upload has already been removed so only need to look for the date here, not time
            else:
                daily_accum = 0
            current_date = this_date
        else:
            daily_accum = daily_accum + measurements['sum_rain'].iloc[i]
        print(daily_accum)
        data['dateutc'] = str(measurements['time_utc_rounded'].iloc[i]).replace(':', '%3A').replace(' ', '+')
        data['rainin'] = str(measurements['Rain'].iloc[i] * 0.039370079)
        data['dailyrainin'] = str(daily_accum * 0.039370079)
        data['tempf'] = str(measurements['Temperature'].iloc[i] *1.8 + 32) # Converting Celsius to Fahrenheit
        data['humidity'] = str(measurements['Humidity'].iloc[i]) 
        # response = requests.post(url, data=data)
        # if response.status_code == 200:
        if 1:
            this_uploaded_data = pd.DataFrame({'location': location, 
                                                'time': measurements['time_utc_rounded'].iloc[i], 
                                                'Rain' : measurements['Rain'].iloc[i],
                                                'sum_rain' : float(daily_accum),
                                                'Temperature' : measurements['Temperature'].iloc[i],
                                                'Humidity' : measurements['Humidity'].iloc[i]},
                                                columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity'], index = [0])
            uploaded_data = uploaded_data.append(this_uploaded_data)
        else:
            this_failed_data = pd.DataFrame({'location': location, 
                                                'time': measurements['time_utc_rounded'].iloc[i], 
                                                'Rain' : measurements['Rain'].iloc[i],
                                                'sum_rain' : daily_accum,
                                                'Temperature' : measurements['Temperature'].iloc[i],
                                                'Humidity' : measurements['Humidity'].iloc[i]},
                                                columns=['location','time', 'Rain','sum_rain', 'Temperature', 'Humidity'], index = [0])
            failed_data = failed_data.append(this_failed_data)
    
    uploaded_data.to_csv('uploaded_data.csv')
    failed_data.to_csv('failed_data.csv')
    tmp = uploaded_data
    tmp = tmp.groupby(tmp['time'].dt.date).last().reset_index(drop=True)
    upload_log_new = upload_log_last.append(tmp)    
    upload_log_new['time'] = pd.to_datetime(upload_log_new['time'])
    # upload_log_new = upload_log_new.groupby(upload_log_new['time'].dt.date).last().reset_index(drop=True)
    upload_log_new.to_csv('upload_log.csv', index = False)


