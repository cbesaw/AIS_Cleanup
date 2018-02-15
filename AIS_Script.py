# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 09:43:34 2018
Master script for AIS Cleaning
@author: OEFDataScience
"""
#packages
import os
import pandas as pd
import glob


###########################################
############################################
#Functions in order of operation

#Only run if you need to convert tmp files to csv
#funky operations, so ignore the warnings. 
def file_convert(input_folder, old_type, new_type):
    folder = input_folder   
    for filename in os.listdir(folder):
            infilename = os.path.join(folder,filename)
            if not os.path.isfile(infilename): continue
            oldbase = os.path.splitext(filename)
            newname = infilename.replace(old_type, new_type)
            output = os.rename(infilename, newname)
            
#retitle sheets into time ranges
def csv_retitle(directory_path, output_path):
    for fname in glob.glob(directory_path):
        #import sheet
        df = pd.read_csv(fname,
                         engine = 'python',
                         error_bad_lines = False)
        #create time object
        df['date_time'] = pd.to_datetime(df['dt_pos_utc'], 
          errors='coerce')
        #find start and end
        #create file name
        start = min(df['date_time']).strftime('%Y-%m-%d')
        end = max(df['date_time']).strftime('%Y-%m-%d')
        name = 'AIS_%s_%s.csv' % (start, end)
        output_root = output_path
        file_name = output_root + name
        #export, but prevent overwriting
        if not os.path.isfile(file_name):
            df.to_csv(file_name)
        else:
            df.to_csv(file_name, mode = 'a', header = False)
            
#clean retitled sheets
def retitle_clean(dir_path, output_path):
    for fname in glob.glob(dir_path):
            file_name = fname[54:]
            df1 = pd.read_csv(fname, 
                              low_memory = False)  
            #find and drop dupes
            df1['dupes'] = df1.duplicated(['mmsi', 'vessel_name', 'dt_pos_utc', 'longitude', 
               'latitude'])
            df1 = df1[df1['dupes']==False]
            df1 = df1.drop(columns = ['dupes'])
            #export
            df1.to_csv(output_path + file_name)
            
##sort cleaned retitled sheets into months
#this is necessary because the data is so large
def month_sort(dir_path, output_path):
    #sort code
    for fname in glob.glob(dir_path):
        #import data and make time list
        df = pd.read_csv(fname,
                         low_memory = False)
        df = df.drop(columns = ['Unnamed: 0', 'Unnamed: 0.1'])
        df['date_time'] = pd.to_datetime(df['dt_pos_utc'], 
                  errors='coerce')
        df['month_year'] = df['date_time'].dt.strftime('%Y-%m')
        time_list = list(df['month_year'].unique())
        #use time list to sort into monthly dataframes
        for t in time_list:
            df2 = df[df['month_year']==t]
            csv_name = output_path + 'AIS_' + t + '.csv'
            #export, but prevent overwriting
            if not os.path.isfile(csv_name):
                df2.to_csv(csv_name, index = False)
            else:
                df2.to_csv(csv_name, mode = 'a', header = False, index = False)
                
#clean month sheets for dupes one final time
def month_clean(dir_path, output_path):
    for fname in glob.glob(dir_path):
        df = pd.read_csv(fname, low_memory = False, error_bad_lines = False)
        file_name = fname[57:]
        df['dupes'] = df.duplicated(['mmsi', 'vessel_name', 'dt_pos_utc', 'longitude', 
                     'latitude'])
        df2 = df[df['dupes']==False]
        df2 = df2.drop(columns = ['dupes'])
        #export
        df2.to_csv(output_path + file_name + '.csv', index = False)

##########################################################################################

            
       
     
    


