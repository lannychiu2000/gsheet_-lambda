# Imports
import os
import csv
import re
import time
import pandas as pd
import numpy as np
import datetime
import json
from sqlalchemy import create_engine
import string as strings
from gspread_formatting import *
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy.types import INTEGER,FLOAT,VARCHAR, BOOLEAN, DATETIME
import gspread
import boto3
from sqlalchemy.sql import text
from threading import Thread
import pickle
from arm_utilities import load_credentials, format_strings, convert_to_int

# Global Variables
acc,sh,em,post,ho,use,wo,database,go,look,look_sec = load_credentials()
new_table = True
local_radio_ids = []
non_local_radio_ids = []
range_string = ''
range_string_2 = ''
df_as_list_2 = ''
df_as_list = ''
error_list = []
GSHEET_NAME_ATTRIBUTE = 'name'
GSHEET_ID_ATTRIBUTE = 'id'
GSHEET_MODIFIED_TIME_ATTRIBUTE = 'modifiedTime'


def update_local_radio_tab(url):
    print ('{} INFO: update_local_radio_tab - Starting...'.format(datetime.datetime.now()))
    try:
        json_datas = go
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_datas,scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(url).worksheet("Link to Market Info - Local Radio")
        cell_range = sheet.range(range_string)
        count = 0
        for cell in cell_range:
            cell.value = df_as_list[count]
            count += 1
        sheet.clear()
        sheet.update_cells(cell_range)
        print ('{} INFO: update_local_radio_tab - Done'.format(datetime.datetime.now()))   
    except Exception as e:
        print ('{} ERROR: update_local_radio_tab - {}'.format(datetime.datetime.now(), e))   

def update_non_local_radio_tab(url):
    print ('{} INFO: update_non_local_radio_tab - Starting...'.format(datetime.datetime.now()))
    try:
        json_datas = go
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_datas,scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(url).worksheet("Link to Market Info")
        cell_range_2 = sheet.range(range_string_2)
        count = 0
        for cell in cell_range_2:
            cell.value = df_as_list_2[count]
            count += 1
        sheet.clear()
        sheet.update_cells(cell_range_2)
        print ('{} INFO: update_non_local_radio_tab - Done'.format(datetime.datetime.now()))   
    except Exception as e:
        print ('{} ERROR: update_non_local_radio_tab - {}'.format(datetime.datetime.now(), e))   

def order_upload(order_url,active_client,table_name,adj_cli ,aws_schema,recomendation):
    print ('{} INFO: order_upload - Starting...'.format(datetime.datetime.now()))
    #recomendation_and_promo_code_addition(order_url_for_matching=order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)
    #add_new_order_sheet(order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)
    #delete_new_orders_tab(order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)
    if datetime.datetime.now().hour <100:

        try:
            start = time.time()
            json_datas = go
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json_datas,scope)
            client = gspread.authorize(creds)
            engine=create_engine(post)
          #  print('getting data from gsheet')
            sheet = client.open_by_url(order_url).worksheet("Promo Codes")

            #sheet = workbook.worksheet("Potential Errors")

            #sheet = workbook.worksheet("Promo Codes")
            promo_list = sheet.get_all_values()
            promo_df = pd.DataFrame(promo_list[1:],columns=promo_list[0])

            sheet.update_acell('B1', """=ARRAYFORMULA(IF(ROW(B1:B) = 1,"Vendor ID",iferror(IF(A1:A <> "",IFERROR(VLOOKUP(AF1:AF,'Link to Market Info'!A:M,13,False),IFERROR(VLOOKUP(A1:A,'Link to Market Info'!B:M,12,False), iferror(VLOOKUP(A1:A,'Link to Market Info'!F:M,8,FALSE),(VLOOKUP(A1:A,'Link to Market Info - Local Radio'!B:D,3,FALSE))))),""),AF1:AF)))""")




            #Load All Data after 8:00 p.m.
            sheet_curr = client.open_by_url(order_url).worksheet("Current Year Orders")
            orders_list_curr = sheet_curr.get_all_values()
            orders_df_curr = pd.DataFrame(orders_list_curr[1:],columns=orders_list_curr[0])


            sheet_hist = client.open_by_url(order_url).worksheet("Historical Orders")
            orders_list_hist = sheet_hist.get_all_values()
            orders_df_hist = pd.DataFrame(orders_list_hist[1:],columns=orders_list_hist[0])

            orders_df = pd.concat([orders_df_curr, orders_df_hist],sort=False)


            orders_df.columns = orders_df_curr.columns
            orders_df.replace("",np.NaN,inplace=True)

            for column in ['orders', 'conversions','revenue', 'session', 'downloads_installs', 'discounts','lead_impressions', 'users', 'new_users', 'approvals','funded_loans_amounts',"extra_1","extra_2"]:
                orders_df[column] = orders_df[column].apply(lambda x: x.replace(',','').replace("$","") if type(x) == str else np.NaN)
                orders_df[column] = orders_df[column].astype(float)

            orders_df['date'] = pd.to_datetime(orders_df['date'])

            orders_df.columns = ['date','discount_code','client_name','orders','conversions','revenue','session','downloads_installs',
                                'discounts','lead_impressions','users','new_users','approvals','funded_loans_amounts','tracking_type',
                                'product_type','lead_impression_type','unattributed_orders','session_type','code_leak_date','extra_1',
                                'extra_2','extra_3']
            if new_table == True:
            #    print('hey new table')
                data_type = {'date':DATETIME,
                             'discount_code':VARCHAR,
                             'client_name':VARCHAR,
                             'orders':FLOAT,
                             'conversions':FLOAT,
                             'revenue':FLOAT,
                             'session':FLOAT,
                             'downloads_installs':FLOAT,
                             'discounts':FLOAT,
                             'lead_impressions':FLOAT,
                             'users':FLOAT,
                             'new_users':FLOAT,
                             'approvals':FLOAT,
                             'funded_loans_amounts':FLOAT,
                             'tracking_type':VARCHAR,
                             'product_type':VARCHAR,
                             'lead_impression_type':VARCHAR,
                             'unattributed_orders':FLOAT,
                             'session_type':VARCHAR,
                             'code_leak_date':DATETIME,
                             'extra_1':FLOAT,
                             'extra_2':FLOAT,
                             'extra_3':FLOAT}

                top_orders = orders_df.head(1)
                try:
                    top_orders.to_sql( table_name + "_orders",engine,schema=aws_schema,if_exists='fail',index=False,dtype=data_type)
                except:
                    x = 1
            orders_df.to_csv(table_name+'order_info.csv',index=False)
            session = boto3.Session()
            s3 = session.resource("s3")
            s3.meta.client.upload_file(table_name+'order_info.csv','adresults',table_name+'order_info.csv')
            os.remove(table_name+'order_info.csv')
          #  print('done')

            query_delete = """TRUNCATE """ + aws_schema + "." + table_name + "_orders"

            query_copy = """copy """ + aws_schema + "." + table_name + "_orders" + """
            from 's3://adresults/""" + table_name+"""order_info.csv'
            credentials 'aws_access_key_id=""" + acc + """;aws_secret_access_key=""" + sh + """'
            IGNOREHEADER 1
            EMPTYASNULL
            csv;"""
           # print('starting upload')
            conn = engine.connect()

            conn.execute(text(query_delete).execution_options(autocommit=True))

            conn.execute(text(query_copy).execution_options(autocommit=True))
            conn.close()
            # s3.Object('adresults',table_name+'order_info.csv').delete()
           # print('done done')

            promo_df.replace('',np.NaN,inplace=True)
            promo_df.replace("#N/A",np.NaN,inplace=True)

            promo_df = promo_df[['Show Name', 'Vendor ID', "Client", 'Promo Code', 'Budget Show Name', 'Unique Code', 'Code Leak Date']]

            promo_df.columns = ['show_name','vendor_id','client','promo_code','budget_show_name','unique_code','code_leak_date']

            promo_df['vendor_id'] = pd.Series(promo_df['vendor_id'],dtype ='Int64')
            if new_table == True:
             #   print('hey new table')
                data_type = {
                    'show_name':VARCHAR,
                    'vendor_id': INTEGER,
                    'client':VARCHAR,
                    'promo_code':VARCHAR,
                    'budget_show_name':VARCHAR,
                    'unique_code':VARCHAR,
                    'code_leak_date':DATETIME
                }

                top_promo = promo_df.head(1)
                try:
                    top_promo.to_sql(table_name + "_promo_codes",engine,schema=aws_schema,if_exists='fail',index=False,dtype=data_type)
                except:
                    x = 1
            promo_df.to_csv(table_name + '.csv',index=False)

            session = boto3.Session()
            s3 = session.resource("s3")
            s3.meta.client.upload_file(table_name + '.csv','adresults',table_name + '.csv')

            missing_vendor_id = str(promo_df['vendor_id'].isna().sum())
            total_number = str(len(promo_df))
           # print('done')
            os.remove(table_name + '.csv')
            engine=create_engine(post)

            query_delete = """TRUNCATE """ + aws_schema + "." + table_name + "_promo_codes"

            query_copy = """copy """ + aws_schema + "." + table_name + "_promo_codes" + """
            from 's3://adresults/""" + table_name +""".csv'
            credentials 'aws_access_key_id=""" + acc + """;aws_secret_access_key=""" + sh + """'
            IGNOREHEADER 1
            EMPTYASNULL
            csv;"""
            #print('starting upload')
            conn = engine.connect()

            conn.execute(text(query_delete).execution_options(autocommit=True))

            conn.execute(text(query_copy).execution_options(autocommit=True))
            conn.close()
            #s3.Object('adresults',table_name + '.csv').delete()
            #print('done done')
            if recomendation == True:
                missing_number = recomendation_and_promo_code_addition(order_url_for_matching=order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)
                total_number = str(len(promo_df) + int(missing_number))
                missing_vendor_id = str(promo_df['vendor_id'].isna().sum() + int(missing_number))
            else:
                missing_number = "recomendations needed"
                #missing_number = add_missing_promo_codes(order_url_for_matching=order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)

            #Only Update input tabs if data available
            if len(non_local_radio_ids) > 0:
                update_non_local_radio_tab(order_url)

            if len(local_radio_ids) > 0:
                update_local_radio_tab(order_url)


            #non_local_radio_ids)

            #update_local_radio_tab(order_url)
            #update_non_local_radio_tab(order_url)
            end = time.time()
            minutes = str(int(round((end-start)/60,0))) +":"
            seconds = str(int(round((end-start)%60,0)))
            if len(seconds) == 1:
                seconds = "0" + seconds
            total_time = minutes + seconds

            print ('{} INFO: Done with table {} - Total Time: {}'.format(datetime.datetime.now(), table_name, total_time))            
            error_list.append(table_name + " completed successfully \n " + "Number of New Promo Codes: " + missing_number + "\n" + "Number of Missing Vendor ID's: " + missing_vendor_id + " out of " + total_number + "\n"+ order_url)
        except Exception as e:
            error_list.append(table_name + " has errored: " + str(e) + "\n" + order_url)
            print ('{} ERROR: Error will processing table {}'.format(datetime.datetime.now(), table_name))
            print ('{} ERROR: {}'.format(datetime.datetime.now(), e))

    #elif datetime.datetime.now().hour < 20 & active_client == True:
    elif datetime.datetime.now().hour < 20 and active_client == True:
    #elif datetime.datetime.now().hour < 20 and active_client == True:
        #print('Load 1')
        try:

            start = time.time()
            json_datas = go
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json_datas,scope)
            client = gspread.authorize(creds)
            engine=create_engine(post)
          #  print('getting data from gsheet')
            sheet = client.open_by_url(order_url).worksheet("Promo Codes")

            #sheet = workbook.worksheet("Potential Errors")

            #sheet = workbook.worksheet("Promo Codes")
            promo_list = sheet.get_all_values()
            promo_df = pd.DataFrame(promo_list[1:],columns=promo_list[0])

            sheet.update_acell('B1', """=ARRAYFORMULA(IF(ROW(B1:B) = 1,"Vendor ID",iferror(IF(A1:A <> "",IFERROR(VLOOKUP(AF1:AF,'Link to Market Info'!A:M,13,False),IFERROR(VLOOKUP(A1:A,'Link to Market Info'!B:M,12,False), iferror(VLOOKUP(A1:A,'Link to Market Info'!F:M,8,FALSE),(VLOOKUP(A1:A,'Link to Market Info - Local Radio'!B:D,3,FALSE))))),""),AF1:AF)))""")





            #sheet = client.open_by_url(order_url).worksheet("Orders")

            #orders_list = sheet.get_all_values()
            #orders_df = pd.DataFrame(orders_list[1:],columns=orders_list[0])

            #Load All Data after 8:00 p.m.

            sheet_curr = client.open_by_url(order_url).worksheet("Current Year Orders")
            orders_list_curr = sheet_curr.get_all_values()
            orders_df_curr = pd.DataFrame(orders_list_curr[1:],columns=orders_list_curr[0])

            orders_df_curr.replace("",np.NaN,inplace=True)

            for column in ['orders', 'conversions','revenue', 'session', 'downloads_installs', 'discounts','lead_impressions', 'users', 'new_users', 'approvals','funded_loans_amounts',"extra_1","extra_2"]:
                orders_df_curr[column] = orders_df_curr[column].apply(lambda x: x.replace(',','').replace("$","") if type(x) == str else np.NaN)
                orders_df_curr[column] = orders_df_curr[column].astype(float)

            #select * from production.client_order_data.ava_science_orders where date <= '1/1/2020'
            #sheet_hist = client.open_by_url(order_url).worksheet("Historical Orders")
            #orders_list_hist = sheet_hist.get_all_values()
            #orders_df_hist = pd.DataFrame(orders_list_hist[1:],columns=orders_list_hist[0])
            #print("""select * from production.client_order_data.""" + table_name + """_orders where date < '1/1/2020'""")
            engine = create_engine(post)
            orders_df_hist = pd.read_sql("""select * from production.client_order_data.""" + table_name + """_orders where date_part('year', date) < date_part('year', current_date)""",engine)

            orders_df = pd.concat([orders_df_curr, orders_df_hist],sort=False)

            orders_df.columns = orders_df_curr.columns
    #        orders_df.replace("",np.NaN,inplace=True)

            orders_df['date'] = pd.to_datetime(orders_df['date'])

#            for column in ['orders', 'conversions','revenue', 'session', 'downloads_installs', 'discounts','lead_impressions', 'users', 'new_users', 'approvals','funded_loans_amounts',"extra_1","extra_2"]:
#                orders_df[column] = orders_df[column].apply(lambda x: x.replace(',','').replace("$","") if type(x) == str else np.NaN)
#                orders_df[column] = orders_df[column].astype(float)



            orders_df.columns = ['date','discount_code','client_name','orders','conversions','revenue','session','downloads_installs',
                    'discounts','lead_impressions','users','new_users','approvals','funded_loans_amounts','tracking_type',
                    'product_type','lead_impression_type','unattributed_orders','session_type','code_leak_date','extra_1',
                    'extra_2','extra_3']


            if new_table == True:
            #    print('hey new table')
                data_type = {'date':DATETIME,
                             'discount_code':VARCHAR,
                             'client_name':VARCHAR,
                             'orders':FLOAT,
                             'conversions':FLOAT,
                             'revenue':FLOAT,
                             'session':FLOAT,
                             'downloads_installs':FLOAT,
                             'discounts':FLOAT,
                             'lead_impressions':FLOAT,
                             'users':FLOAT,
                             'new_users':FLOAT,
                             'approvals':FLOAT,
                             'funded_loans_amounts':FLOAT,
                             'tracking_type':VARCHAR,
                             'product_type':VARCHAR,
                             'lead_impression_type':VARCHAR,
                             'unattributed_orders':FLOAT,
                             'session_type':VARCHAR,
                             'code_leak_date':DATETIME,
                             'extra_1':FLOAT,
                             'extra_2':FLOAT,
                             'extra_3':FLOAT}

                top_orders = orders_df.head(1)
                try:
                    top_orders.to_sql( table_name + "_orders",engine,schema=aws_schema,if_exists='fail',index=False,dtype=data_type)
                except:
                    x = 1


            #print(orders_df)

            orders_df.to_csv(table_name+'order_info.csv',index=False)
            session = boto3.Session()

            s3 = session.resource("s3")
            s3.meta.client.upload_file(table_name+'order_info.csv','adresults',table_name+'order_info.csv')
            os.remove(table_name+'order_info.csv')

          #  print('done')

            query_delete = """TRUNCATE """ + aws_schema + "." + table_name + "_orders"

            #print(query_delete)
            query_copy = """copy """ + aws_schema + "." + table_name + "_orders" + """
            from 's3://adresults/""" + table_name+"""order_info.csv'
            credentials 'aws_access_key_id=""" + acc + """;aws_secret_access_key=""" + sh + """'
            IGNOREHEADER 1
            EMPTYASNULL
            csv;"""

            #print(query_copy)
           # print('starting upload')
            conn = engine.connect()

            conn.execute(text(query_delete).execution_options(autocommit=True))

            conn.execute(text(query_copy).execution_options(autocommit=True))
            conn.close()
            # s3.Object('adresults',table_name+'order_info.csv').delete()
           # print('done done')

            promo_df.replace('',np.NaN,inplace=True)
            promo_df.replace("#N/A",np.NaN,inplace=True)

            promo_df = promo_df[['Show Name', 'Vendor ID', "Client", 'Promo Code', 'Budget Show Name', 'Unique Code', 'Code Leak Date']]

            promo_df.columns = ['show_name','vendor_id','client','promo_code','budget_show_name','unique_code','code_leak_date']

            promo_df['vendor_id'] = pd.Series(promo_df['vendor_id'],dtype ='Int64')
            if new_table == True:
             #   print('hey new table')
                data_type = {
                    'show_name':VARCHAR,
                    'vendor_id': INTEGER,
                    'client':VARCHAR,
                    'promo_code':VARCHAR,
                    'budget_show_name':VARCHAR,
                    'unique_code':VARCHAR,
                    'code_leak_date':DATETIME
                }

                top_promo = promo_df.head(1)
                try:
                    top_promo.to_sql(table_name + "_promo_codes",engine,schema=aws_schema,if_exists='fail',index=False,dtype=data_type)
                except:
                    x = 1
            promo_df.to_csv(table_name + '.csv',index=False)

            session = boto3.Session()
            s3 = session.resource("s3")
            s3.meta.client.upload_file(table_name + '.csv','adresults',table_name + '.csv')

            missing_vendor_id = str(promo_df['vendor_id'].isna().sum())
            total_number = str(len(promo_df))
           # print('done')
            os.remove(table_name + '.csv')
            engine=create_engine(post)

            query_delete = """TRUNCATE """ + aws_schema + "." + table_name + "_promo_codes"

            query_copy = """copy """ + aws_schema + "." + table_name + "_promo_codes" + """
            from 's3://adresults/""" + table_name +""".csv'
            credentials 'aws_access_key_id=""" + acc + """;aws_secret_access_key=""" + sh + """'
            IGNOREHEADER 1
            EMPTYASNULL
            csv;"""
            #print('starting upload')
            conn = engine.connect()

            conn.execute(text(query_delete).execution_options(autocommit=True))

            conn.execute(text(query_copy).execution_options(autocommit=True))
            conn.close()
            #s3.Object('adresults',table_name + '.csv').delete()
            #print('done done')
            if recomendation == True:
                missing_number = recomendation_and_promo_code_addition(order_url_for_matching=order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)
                total_number = str(len(promo_df) + int(missing_number))
                missing_vendor_id = str(promo_df['vendor_id'].isna().sum() + int(missing_number))
            else:
                missing_number = "recomendations needed"
                #missing_number = add_missing_promo_codes(order_url_for_matching=order_url,json_data_input=go,table_name_for_matching=table_name,adjusted_client=adj_cli)

            #Only Update input tabs if data available
            if len(non_local_radio_ids) > 0:
                update_non_local_radio_tab(order_url)

            if len(local_radio_ids) > 0:
                update_local_radio_tab(order_url)


            #non_local_radio_ids)

            #update_local_radio_tab(order_url)
            #update_non_local_radio_tab(order_url)
            end = time.time()
            minutes = str(int(round((end-start)/60,0))) +":"
            seconds = str(int(round((end-start)%60,0)))
            if len(seconds) == 1:
                seconds = "0" + seconds
            total_time = minutes + seconds

            print ('{} INFO: Done with table {} - Total Time: {}'.format(datetime.datetime.now(), table_name, total_time))
            error_list.append(table_name + " completed successfully \n " + "Number of New Promo Codes: " + missing_number + "\n" + "Number of Missing Vendor ID's: " + missing_vendor_id + " out of " + total_number + "\n"+ order_url)
        except Exception as e:
            error_list.append(table_name + " has errored: " + str(e) + "\n" + order_url)
            print ('{} ERROR: Error will processing table {}'.format(datetime.datetime.now(), table_name))
            print ('{} ERROR: {}'.format(datetime.datetime.now(), e))
    print ('{} INFO: order_upload - Done'.format(datetime.datetime.now()))

def handler(event, context):
    global local_radio_ids
    global non_local_radio_ids
    global range_string
    global range_string_2
    global df_as_list_2
    global df_as_list

    print ('{} INFO: handler - Starting...'.format(datetime.datetime.now()))
    start = time.time()
    engine = create_engine(post)

    local_radio_ids = pd.read_sql("""SELECT client_name,combined,date_added,pseudo_vendor_id FROM matt_testing.local_radio_pseudo_id""",engine)

    print ('{} INFO: handler - About to make df_as_list'.format(datetime.datetime.now()))
    df_as_list = ["client_name","combined","date_added","pseudo_vendor_id"]
    for row in range(len(local_radio_ids)):
        str_list = ["" if pd.isnull(x) else float(x) if type(x) == np.float64 else int(x) if type(x) == np.int64 else str(x) for x in list(local_radio_ids.iloc[row])]
        df_as_list += str_list
    range_string = "A1:D"+ str(len(local_radio_ids) + 1)
    
    non_local_radio_ids = pd.read_sql("""select vendor_id, station_name, market_name, Media_Type, Adjusted_Market_Name, Adjusted_Show_Name, Genre_itunes, Genre_ARM, Subgenre_iTunes, Subgenre_ARM, Itunes_URL, Unique_show_flag, Master_vendor_id from production.gsheet.unique_shows""",engine)

    print ('{} INFO: handler - About to make df_as_list_2'.format(datetime.datetime.now()))
    df_as_list_2 = ["vendor_id","station_name","market_name","Media_Type","Adjusted_Market_Name","Adjusted_Show_Name","Genre_itunes","Genre_ARM","Subgenre_iTunes","Subgenre_ARM","Itunes_URL","Unique_show_flag","Master_vendor_id"]
    for row in range(len(non_local_radio_ids)):
        str_list_2 = ["" if pd.isnull(x) else float(x) if type(x) == np.float64 else int(x) if type(x) == np.int64 else str(x) for x in list(non_local_radio_ids.iloc[row])]
        df_as_list_2 += str_list_2
    range_string_2 = "A1:M"+ str(len(non_local_radio_ids) + 1)

    for record in event['Records']:
        messageAttributes=record["messageAttributes"]
        gSheetID = messageAttributes[GSHEET_ID_ATTRIBUTE].get('stringValue')
        gSheetName = messageAttributes[GSHEET_NAME_ATTRIBUTE].get('stringValue')
        gSheetModifiedTime = messageAttributes[GSHEET_MODIFIED_TIME_ATTRIBUTE].get('stringValue')

        order_url = 'https://docs.google.com/spreadsheets/d/' + gSheetID + '/edit#gid=2096436519'
        order_url = 'https://docs.google.com/spreadsheets/d/1hM6lPySowvOrnbKgQWtJrFjUFw0zkVcldWdZM9ghxLk/edit#gid=2096436519'
        active_client = True
        table_name = 'fitbod'
        adj_cli = 'Fitbod'
        aws_schema = 'client_order_data'
        recomendation = False

        print ('{} INFO: handler - Loading data for table {}, please wait...'.format(datetime.datetime.now(), table_name))
        order_upload(order_url,active_client,table_name,adj_cli ,aws_schema,recomendation)
        print ('{} INFO: handler - Done...'.format(datetime.datetime.now()))

if __name__ == '__main__':
  event = ''
  handler(event,'')


