#This script reads in data from a database as well as multiple smartsheets through the smartsheet API, compares the differences in the data, 
#and then writes the more current database data back to smartsheet for the users

import pandas as pd
import numpy as np
import smartsheet as ss

proxy = 'proxy_server'
token = 'smartsheet_token'
engine = 'database_engine'

#This following functions convert the data received from the api in json format to a pandas dataframe
def get_columns(ss):
    cl = ss.get_columns(include_all=True)
    d3 = cl.to_dict()
    cols = [x['title'] for x in d3['data']]
    return ['row_id', 'last_modified'] + cols

def get_values(ss):
    d = ss.to_dict()
    drows = d['rows']
    rows = [x['cells'] for x in drows]
    values = [[x['value'] if 'value' in x.keys() else None for x in y] for y in rows]
    ids = [[x['id'],x['modifiedAt']] for x in drows]
    values2 = [x + y for x, y in zip(ids, values)]
    return pd.DataFrame(values2)

def get_sheet_as_df(sheet_id, ss_client):
    ss1 = ss_client.Sheets.get_sheet(sheet_id)
    df = get_values(ss1)
    s2 = get_columns(ss1)
    df.columns = s2
    return df


if __name__ == "__main__":

    ss_client = ss.Smartsheet(token, proxies=proxy)

    print('\n Reading in data from database...', end='\r')
    #Read in data from the database and perform data cleanup
    query = """SELECT program, project_id, activity_id, col1, col2, col3, col4, col5, col6, date1, date2, date3, date4
                FROM schema.table"""
                        
    df = pd.read_sql(query, con=engine)
    print('Reading in data from database...Complete.', end='\n')
    
    print('Performing data cleanup...', end='\r')
    #Create date5 by filling in null date1 with date2 and null date3 with date4 where type is type1
    df['date5'] = df['date1'].fillna(df['date2'])
    df['date5'].loc[df['type']=='type1'] = df['date3'].fillna(df['date4'])
    
    #Convert all dates to a datetime date and then to a string in order to write them to the api
    date_lst = ['date1', 'date2', 'date3', 'date4', 'date5']
    for date in date_lst:
        df[date] = pd.to_datetime(df[date]).dt.date
        df[date] = df[date].astype(str)

    #Create the unique_id and then group by it to get the rate for each object
    df['unique_id'] = df['project_id'] + ': ' + df['activity_id']
    df1 = df[['unique_id','col1']].groupby(['unique_id'], as_index=False)['col1'].agg(['sum','count']).reset_index()
    df1['rate'] = df1['sum']/df1['count']
    df = pd.merge(df, df1, how='left', on='unique_id', sort=False)

    #Use the rate to assign a status
    df['status'] = 'Not Complete'
    df['status'].loc[(df['rate'].isnull())|(df['rate']==1)] = 'Complete'

    df = df[['unique_id','program','project_id','activity_id','col1','col2','col3','col4', 
                'col5','col6','date3','date4','date5','status']].drop_duplicates()
    print('Performing data cleanup...Complete.', end='\n')

    #Dictionary of sheet IDs for each program
    program_dct = {'program1':111111111111111,
                  'program2':222222222222222,
                  'program3':333333333333333,
                  'program4':444444444444444,
                  'program5':555555555555555}

    for program,sheet_id in program_dct.items():
        #Read in the data from smartsheet and convert to a pandas dataframe
        sheet = ss_client.Sheets.get_sheet(sheet_id)
        col_dct = {}
        for col in sheet.columns:
            col_dct[col.title] = col.id
        ss_df = get_sheet_as_df(sheet_id, ss_client)
        
        #Keep just the data from the database for this particular program
        program_df = df[df['program'] == program].drop(['program'],axis=1)
        
        #Compare the smartsheet data to the database data in order to update, insert, or delete in smartsheet
        comp_df = pd.concat([ss_df[[x for x in program_df.columns]].fillna('').apply(lambda x: x.str.strip()),program_df.apply(lambda x: x.str.strip())],axis=0).drop_duplicates(keep=False)
        if comp_df.empty:
            continue
        else:
            df = pd.merge(comp_df[['unique_id']].drop_duplicates(), ss_df[['unique_id','row_id']], how='left', on='unique_id', sort=False)
            df = pd.merge(df, program_df, how='left', on='unique_id', sort=False)
            update_df = df[(df['row_id'].notnull())&(df['project_id'].notnull())]
            insert_df = df[df['row_id'].isnull()].drop(['row_id'], axis=1)
            delete_df = df[df['project_id'].isnull()]

            if not update_df.empty:
                print('Updating ' + program + '...', end='\r')
                update_rows_dct = {}
                for index,row in update_df.iterrows():
                    update_rows_dct[row['row_id']] = {col_dct['col1']:row['col1'],
                                                     col_dct['col2']:row['col2'],
                                                     col_dct['col3']:row['col3'],
                                                     col_dct['date4']:row['date4'],
                                                     col_dct['date5']:row['date5'],
                                                     col_dct['status']:row['status']}
                try:
                    update_rows_lst = []
                    for row, cols in update_rows_dct.items():
                        # Build the row to update
                        new_row = ss_client.models.Row()
                        new_row.id = row
                        for col,value in cols.items():
                            # Build new cell value
                            new_cell = ss_client.models.Cell()
                            new_cell.column_id = col
                            if col in ['date4','date5']:
                                new_cell.objectValue = {'objectType':'DATE','values':value}
                            new_cell.value = value
                            new_cell.strict = False

                            #append to new row
                            new_row.cells.append(new_cell)
                        update_rows_lst.append(new_row)

                    # Update rows
                    response = ss_client.Sheets.update_rows(sheet_id, update_rows_lst)
                    print('Updating ' + program + '...Complete.', end='\n')
                except Exception as e:
                    print(e)

            if not insert_df.empty:
                print('Inserting ' + program + '...', end='\r')
                try:
                    insert_rows_lst = []
                    for index, row in insert_df.iterrows():
                        # Build the row to update
                        new_row = ss_client.models.Row()
                        new_row.to_bottom = True
                        for col in insert_df.columns.tolist():
                            new_cell = ss_client.models.Cell()
                            new_cell.column_id = col_dct[col]
                            new_cell.strict = False
                            if col in ['date3','date4','date5']:
                                new_cell.objectValue =  {'objectType':'DATE','values':[row[col]]}
                                new_cell.value = row[col]
                            else:
                                new_cell.value = row[col]

                            new_row.cells.append(new_cell)
                        insert_rows_lst.append(new_row)

                    #Insert rows
                    response = ss_client.Sheets.add_rows(sheet_id, insert_rows_lst)
                    print('Inserting ' + program + '...Complete.', end='\n')
                except Exception as e:
                    print(e)

            if not delete_df.empty:
                print('Deleting ' + program + '...', end='\r')
                try:
                    if '.' in delete_df['row_id'].astype(str).all():
                        delete_lst = delete_df['row_id'].astype(str).str[:-2].tolist()
                    else:
                        delete_lst = delete_df['row_id'].tolist()
                    #The smartsheet api only lets you delete 200 rows at a time so this loops through them in 200 row increments
                    if len(delete_lst) > 200:
                        while len(delete_lst) > 200:
                            new_lst = delete_lst[:200]
                            response = ss_client.Sheets.delete_rows(sheet_id, new_lst)
                            delete_lst = delete_lst[200:]
                        response = ss_client.Sheets.delete_rows(sheet_id, delete_lst)
                    else:
                        response = ss_client.Sheets.delete_rows(sheet_id, delete_lst)
                    print('Deleting ' + program + '...Complete.', end='\n')
                except Exception as e:
                    print(e)
