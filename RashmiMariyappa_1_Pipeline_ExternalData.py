"""
_______________________________________________________________________________
-------------------------------------------------------------------------------
Uploading Third-Party Data to EDW

Please Note: Various pieces of this code have been removed for privacy reasons.
This is indicated with a ...

Created By: Rashmi M (updating/changing previous team member's code)

Description: 
This code extracts specific metrics for third party data from .csv flat-files
and uploads the data into the EDW 
   

-------------------------------------------------------------------------------
Instructions: 
1. Update the USER INPUT VARIABLES.
2. Run all the IMPORT STATMENTS.
3. Run the FUNCTIONS.
4. Run the first portion of the MAIN PROGRAM ...
5. Run the second portion of the MAIN PROGRAM ...
6. While the type_run variable is set to "development" ...
7. Validate the data ...
8. As of ..., the data from the development table is ...
9. Check the data uploaded into the production table...

_______________________________________________________________________________
-------------------------------------------------------------------------------
MEASURE NAMES

These are the measure names that we are extracting and uploading. This is being
listed here for documentation purposes only. The file indexes listed below
slightly change based on file type.

-------------------------------------------------------------------------------
***Outcomes Files Indexes

New Row Indexing - These are the new indexes that took effect 5/2022
current_row_index = [10, 42, 44, 51, 53, 209, 210, 216, 217, 335, 343, 347, 366]

Old Row Indexing - These were the old row indexes that were being used prior
old_row_index = [10, 42, 44, 51, 53, 131, 132, 138, 139, 257, 265, 269, 288] 

Row Index [Old Row Index] - "Name of Measure"
...

-------------------------------------------------------------------------------
***Percentile Rank Files Indexes

Row Indexing - These are the percentile rank indexes that started to be 
incorporated in ...
current_row index = [27, 28, 29, 30, 42, 43, 48, 49]

Row Index - "Name of Measure"
...

-------------------------------------------------------------------------------
_______________________________________________________________________________
"""



###############################################################################
""" ----- IMPORT STATEMENTS ----- """
import pandas as pd
import numpy as np
import psycopg2
import shutil
import os
import warnings
warnings.filterwarnings('ignore')
from datetime import date



###############################################################################
""" ----- USER INPUT VARIABLES ----- """
# The below variables should be checked and updated by the user for each run.
# These variables include files paths, ..., run type, and month info. 

#------------------------------------------------------------------------------
# FY - Only Change When Fiscal Year Updates

# Path to Previous Fiscal Year Source Directory 
orig_dir_pfy = "file path"

# Path to Previous Fiscal Year Destination Directory
copy_dir_pfy = "file path"

#------------------------------------------------------------------------------
# Path to Fiscal Year to Date Year Source Directory
orig_dir_fytd = "file path"

# Path to Fiscal Year to Date Year Destination Directory
copy_dir_fytd = "file path"

# Path to Export Directory
export_dir_fytd = "file path"

#------------------------------------------------------------------------------
#Setting the Date of the Data to Be Uploaded
date_text="date" 

#Setting the Date for File Names
file_date="date" 

#Setting the Time Period for Current Time Period
fytd_text = "Time Period"

#Setting the Time Period for Previous Time Period
pfy_text = "Time Period"

#Enter your ... and ...
...

#Is this being run into the production or development table?
type_run = "development"
#type_run = "production"



###############################################################################
""" ----- FUNCTIONS ----- """
#______________________________________________________________________________
### duplicate_rename_file
# The duplicate_rename_file, copies the original files and renames them
# according to the floor/facility it is for.

def duplicate_rename_file(src_dir, dest_dir):
    files = os.listdir(src_dir)
    shutil.copytree(src_dir, dest_dir)   
    
    for f in files:
    
        if f.split('_')[0] == "FacilityPercentile":
            df = pd.read_csv(dest_dir +"\\" + f)                                  
            unit = df.iloc[2,0]                                                       
            col_name = ""
        
            if unit == "Payer: ...":
                col_name = "..._Fac..._Pct"
            elif unit == "Payer: All":
                col_name = "..._FacAll_Pct"
           
            rename = "3_" + col_name + "_FYPY_" + file_date + ".csv"
            os.rename(dest_dir + "\\" + f, dest_dir + "\\" + rename)  
        
        
        if f.split('_')[0] == "FacilityReport":  
            df = pd.read_csv(dest_dir +"\\" + f)   
            unit = df.iloc[0,0]                                                         
            col_name = ""                                                          
        
            if unit == "Facility-Specific outcomes for: Floor: ...!":
                col_name = "..._Floor...!"
            elif unit == "Facility-Specific outcomes for: Floor: ...@":
                col_name = "..._Floor...@"
            elif unit == "Facility-Specific outcomes for: Floor: ...#":
                col_name = "..._Floor...#"
            elif unit == "Facility-Specific outcomes for: Floor: ...$":
                col_name = "..._Floor...$"
            elif unit == "Facility-Specific outcomes for: Floor: ...%":
                col_name = "..._Floor...%"
            elif unit == "Facility-Specific outcomes for: Floor: ...^":
                col_name = "..._Floor...^"
            elif unit == "Facility-Specific outcomes for: Floor: ...&":
                col_name = "..._Floor...*"
            elif unit == "Facility-Specific outcomes for: Floor: ...*":
                col_name = "..._Floor...*"
            elif unit == "Facility-Specific outcomes for: Floor: ...**":
                col_name = "..._Floor...**"
            else:                                                                      
                other = df.iloc[3, 0]                                                   
                if other == "Payer: ...":                                          
                    col_name = "..._Facility..."
                elif other == "Payer: All":
                    col_name = "..._FacilityAll"

            rename = "1_" + col_name + "_FYPY_" + file_date + ".csv"
            os.rename(dest_dir + "\\" + f, dest_dir + "\\" + rename)


        if f.split('_')[0] == "...GroupDetail":
            df = pd.read_csv(dest_dir +"\\" + f)                                  
            unit = df.columns[0]                                                       
            col_name = ""
        
            if unit == "Outcomes Report - (C1)":
                col_name = "..._C1"
            elif unit == "Outcomes Report - (C2)":
                col_name = "..._C2"
            elif unit == "Outcomes Report - (C3)":
                col_name = "..._C3"
            elif unit == "Outcomes Report - (C4)":
                col_name = "..._C4"
            elif unit == "Outcomes Report - (C5)":
                col_name = "..._C5"
            elif unit == "Outcomes Report - (C6)":
                col_name = ".._C6"

            rename = "2_" + col_name + "_FYPY_" + file_date + ".csv"
            os.rename(dest_dir + "\\" + f, dest_dir + "\\" + rename)                    
    
    return    


#______________________________________________________________________________
### create_dir
# This is the function that creates the directory/folder for export data

def create_dir(export_dir_create):
    export_dir_used = export_dir_create
    
    try: 
        os.mkdir(export_dir_used)
    
    except OSError as error: 
        print(error)  
    
    return


#______________________________________________________________________________
### find_time_period
# The find_time_period function reads the first 5 cells in the data frame
# and finds the cell that begins with "Time_Period:" and return this cell value.
def find_time_period(df):
    time_period=""
    for i in range (5):
        cell = str(df.iloc[i, 0])
        portion_cell = cell[0:12]
        if portion_cell == "Time Period:":
            time_period=str(df.iloc[i, 0])
    return time_period
    
    
#______________________________________________________________________________
### upload_dt
# The upload_dt function creates a column in the dataframe called upload_date
# and enters the current date as the value.

def upload_dt(df):
    today = date.today()
    df['upload_date'] = str(today)
    return df
#______________________________________________________________________________
### find_unit_fypy
# The find_unit_fypy function returns the row_index for the measures needed as
# well as the type of data ... as col_name. This is used for the 
# ... files only.

def find_unit_fypy(df, time_period):
    
    # This will be the floor number or nan if for the entire facility
    unit = str(df.iloc[0,0])
    
    # Setting the row index for measures to be captured based on the file
    # Current Year Indexes
    if time_period == fytd_text and unit != "nan":
        row_index = [10, 42, 44, 51, 53, 209, 210, 216, 217, 335, 343, 347, 366]
        
    elif time_period == fytd_text and unit == "nan":
         row_index = [9, 41, 43, 50, 52, 208, 209, 215, 216, 334, 342, 346, 365]
    
    # Previous Year Indexes
    elif time_period == pfy_text and unit != "nan":
        row_index = [10, 42, 44, 51, 53, 209, 210, 216, 217, 335, 343, 347, 366]
    
    elif time_period == pfy_text and unit == "nan":
        row_index = [9, 41, 43, 50, 52, 208, 209, 215, 216, 334, 342, 346, 365]
    
    else:
        row_index = []

    # Setting the col_name variable for type of data being recorded
    if unit == "Facility-Specific outcomes for: Floor: ...!":
        col_name = "..._floor_...!"
    elif unit == "Facility-Specific outcomes for: Floor: ...@":
        col_name = "..._floor_...@"
    elif unit == "Facility-Specific outcomes for: Floor: ...#":
        col_name = "..._floor_...#"
    elif unit == "Facility-Specific outcomes for: Floor: ...$":
        col_name = "..._floor_...$"
    elif unit == "Facility-Specific outcomes for: Floor: ...%":
        col_name = "..._floor_...%"
    elif unit == "Facility-Specific outcomes for: Floor: ...^":
        col_name = "..._floor_...^"
    elif unit == "Facility-Specific outcomes for: Floor: ...&":
        col_name = "..._floor_...&"
    elif unit == "Facility-Specific outcomes for: Floor: ...*":
        col_name = "..._floor_...*"
    elif unit == "Facility-Specific outcomes for: Floor: ...**":
        col_name = "..._floor_...**"
    elif unit == "nan":
        payer = df.iloc[3, 0]
        if payer == "Payer: ...":                                         
            col_name = "..._facility_..."
        elif payer == "Payer: All":
            col_name = "..._facility"
    else:
        col_name = ""
    
    return col_name, row_index


#______________________________________________________________________________
### find_group - (Not Needed for Fiscal Year)
# The find_group function, based on the file type, gets the patient group type
# from the file as well as the row indexes of the metrics of concern and returns 
# these as the "col_name" and "row_index" variables, respectively. This is used
# for the ... group files.

def find_group_fypy(df, time_period):
    
    # This will be the ric group for the data
    unit = df.columns[0]
    
    # Setting the row index for measures to be captured based on the file
    # Current Year Indexes
    if time_period == fytd_text:
        row_index = [9, 41, 43, 50, 52, 208, 209, 215, 216, 334, 342, 346, 365]
        
    # Previous Year Indexes
    elif time_period == pfy_text:
        row_index = [9, 41, 43, 50, 52, 208, 209, 215, 216, 334, 342, 346, 365]
    
    else:
        row_index = []
    
        
    # Setting the col_name variable for type of data being recorded
    if unit == "Outcomes Report - ... (C1)":
        col_name = "..._C1"
    elif unit == "Outcomes Report - ... (C2)":
        col_name = "..._C2"
    elif unit == "Outcomes Report - ...(C3)":
        col_name = "..._C3"
    elif unit == "Outcomes Report - ... (C4)":
        col_name = "..._C4"
    elif unit == "Outcomes Report - ... (C5)":
        col_name = "..._C5"
    elif unit == "Outcomes Report - ... (C6)":
        col_name = "..._C6"
    else:
        col_name = ""
           
    return col_name, row_index



#______________________________________________________________________________
### find_pct_index
# The find_pct_index function, finds the row_index for the measures needed as
# well as the type of data ... as col_name. This is used for the 
# percent rank files only.

def find_pct_index_fypy(df, time_period):
    
    if time_period == fytd_text or time_period == pfy_text:
        
        # Setting the row index which is the same for both files
        row_index = [27, 28, 29, 30, 42, 43, 48, 49]
    
        # Setting the col_name variable for type of data being recorded
        payer = df.iloc[2, 0]
        if payer == "Payer: ...":                                         
            col_name = "..._facility_..."
        elif payer == "Payer: All":
            col_name = "..._facility"
        else:
            col_name = ""
    else:
        print("There is a problem with the find_pct_index_fypy function")
           
    return col_name, row_index


#______________________________________________________________________________
### select_rows
# The select_row function, based on the file type, returns a subset of the 
# dataframe that contains only the rows in the dataframe of the row_index and 
# relabels the columns of the data as well as adds identifying columns and 
#returns the subsetted dataframe. This is used for the outcome files.

def select_rows_fypy(df, row_index, group, time_period, date):
    df = df.iloc[row_index, [0, 1, 2]]

    if time_period == fytd_text:
        col_names = ['measure', '..._fytd', 'weighted_national_fytd']
        
    elif time_period == pfy_text:
        col_names = ['measure', '..._pfy', 'weighted_national_pfy']
    
    df.columns = col_names
    df.loc[:,'unit'] = group
    df.loc[:,'date'] = date_text
    return df


#______________________________________________________________________________
### select_rows_pct
# The select_row_pct function, based on the file type, returns a subset of the 
# dataframe that contains only the rows in the dataframe of the row_index and 
# relabels the columns of the data as well as adds identifying columns and 
#returns the subsetted dataframe. This is used for the percentile files.


def select_rows_pct_fypy(df, row_index, group, time_period, date):
    df = df.iloc[row_index, [0, 4]]

    if time_period == fytd_text:
        col_names = ['measure', '..._pct_rank_fytd']
        
    elif time_period == pfy_text:
        col_names = ['measure', '..._pct_rank_pfy']
    
    df.columns = col_names
    df.loc[:,'unit'] = group
    df.loc[:,'date'] = date_text
    return df


#______________________________________________________________________________
### ..._calc
# The ... function converts the raw number of ... to a percentage based on 
#overall ....

def ..._calc_fypy(df):
   
   ... 
   
    return


#______________________________________________________________________________
### drop_row
# The drop_row drops the 'Discharges...' row

def drop_row(df):
    df = df[df.measure != "Discharges..."]
    return df


#______________________________________________________________________________
### strip_percent
# The strip_percent function removes the "%" from values.

def strip_percent(df):
    for indx, row in df.iterrows():                                             
        for column in row:
            perc_col = (row[row == column].index[0])
            if isinstance(column, str):
                if "%" in column:
                    df.loc[indx, perc_col] = round(float(column.strip('%')), 2)
    return


#______________________________________________________________________________
### rename_pct_measure
# The rename_pct_measure function renames the measures column in the dataframe
# to match the naming in the outcomes files.

def rename_pct_measure(df):
    df['measure_orig'] = df['measure']
    
    for i in range(len(df)):
        
        if df.iloc[i,0] == "Community...":
            df.iloc[i,0] = "Community..."
        
        elif df.iloc[i,0] == "...Subacute...^":
            df.iloc[i,0] = "3 - Skilled..."
        
        elif df.iloc[i,0] == "Acute...":
            df.iloc[i,0] = "Acute..."
        
        elif df.iloc[i,0] == "New...":
            df.iloc[i,0] = "All..."
        
        elif df.iloc[i,0] == "SC1...":
            df.iloc[i,0] = "SC1..."
        
        elif df.iloc[i,0] == "SC2...":
            df.iloc[i,0] = "SC2..."
        
        elif df.iloc[i,0] == "Mobility...":
            df.iloc[i,0] = "Mobility..."
        
        elif df.iloc[i,0] == "MC...":
            df.iloc[i,0] = "MC..."
        
        else:
            print("Measure renaming not found.")
            
    df.drop(['measure_orig'], axis=1, inplace=True)
    
    return df


#______________________________________________________________________________
### export_upload
# This is the function that exports the data that will be uploaded to the EDW
# to a .csv file.

def export_upload_fypy(df):
    export_path = export_dir_fytd + "\\" + "FYPY_Export_Month_" + file_date + ".csv"
    df.to_csv(export_path, index=False)
    return


#______________________________________________________________________________
### make_conn
# The make_conn function creates a connection to the EDW.

def make_conn():
   
   ...

    return myConnection


#______________________________________________________________________________
#### export_to_edw
# This is the function that exports the records to the EDW
# It converts NANs to None type for SQL upload.
# Based on what the type_run variable is set as will determine if it gets run 
# into the development or production data tables.

def export_to_edw(df):
    
    if type_run == "development":
        sql_insert = "INSERT INTO ..." 

    
    elif type_run == "production":
        sql_insert = "INSERT INTO ...;" 
        
    else:
        print('There is a problem with the type_run variable setting.')
    
    df = df.replace(np.nan, None, regex=True)
    insert_values = df.values
    
    try:
        conn = make_conn()
        cur = conn.cursor()
        cur.executemany(sql_insert, insert_values)
    
    except psycopg2.DatabaseError as e:
        return e
    
    finally:
        conn.commit()
        cur.close()
        if conn:
            conn.close()