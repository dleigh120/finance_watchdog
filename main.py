# // Finance.watchdog: Script is intended to be scheduled on a daily basis

# Package management
import yfinance as yf
import pandas as pd 
import xlsxwriter
import datetime as dt
import time
import os
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)

# Variables
tickers = ['BP', 'MAR', 'VAC', 'RIO', 'RWA', 'BARC', 'DBRC', 'EWW', 'IFFF', 'IEUX', 'BOY', 'CME', 'FEVR', 'EMG', 'RELX', 'TCAP', 'REC', 'IGP', 'TRMR', 'RMG']
fromz = 'from@gmail.com'
to = 'to@gmail.com'
sendgrid_api_key = 'XXXXXX'
file_storage_loc = ''

start = time.time()

# Find dates with available close_price based on DOW- no price info over weekend & 1 day data latency
if pd.to_datetime('today').strftime('%w') == '1': 
  current = (pd.to_datetime('today') - pd.Timedelta(days=3))
elif pd.to_datetime('today').strftime('%w') == '0':
  current = (pd.to_datetime('today') - pd.Timedelta(days=2))
else: 
  current = (pd.to_datetime('today') - pd.Timedelta(days=1))

dates_dict = {'three_yrs': current - pd.Timedelta(days=1080), 'one_year': current - pd.Timedelta(days=365), 'three_months': current - pd.Timedelta(days=90), 'one_month': current - pd.Timedelta(days=30), 'one_week': current - pd.Timedelta(days=7), 'three_days': current - pd.Timedelta(days=3), 'one_day': current - pd.Timedelta(days=1)}

dates_dict_cleaned = {}

for k, v in dates_dict.items(): 
  if dates_dict[k].strftime('%w') == '1': 
    dates_dict_cleaned[k] = (dates_dict[k] - pd.Timedelta(days=3))
  elif dates_dict[k].strftime('%w') == '0': 
    dates_dict_cleaned[k] = (dates_dict[k] - pd.Timedelta(days=2))
  elif dates_dict[k].strftime('%w') == '6': 
    dates_dict_cleaned[k] = (dates_dict[k] - pd.Timedelta(days=1))
  else: 
    dates_dict_cleaned[k] = dates_dict[k]
  dates_dict_cleaned[k] = dates_dict_cleaned[k].normalize()

dates_dict_cleaned['current'] = current.normalize()

# Loop through ticker list and add ticker data as dataframe to dictionary
df_dict = {}
error_list_df = pd.DataFrame(columns=['stock', 'error_type', 'value'])

for i in tickers:
  stock = yf.Ticker(i)     
  df = pd.DataFrame(stock.history(period='max'))
  df = df.reset_index()
  if len(df) == 0:
    print('delisted_error: ', i)
    error_list_df = error_list_df.append({'stock': i, 'error_type': 'delisted', 'value': 'NA'},ignore_index=True)    
  elif max(pd.to_datetime(df['Date'],  infer_datetime_format=True)) < dates_dict_cleaned['current']:
    print('date_error: ', i )
    error_list_df = error_list_df.append({'stock': i, 'error_type': 'max_date', 'value': max(pd.to_datetime(df['Date'],  infer_datetime_format=True))},ignore_index=True)
  else: 
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date'],  infer_datetime_format=True)
    df = df.sort_values(by='Date', ascending=False)
    df_dict[i] = df

# Define stock variables
df_alerts = pd.DataFrame()
df_agg = pd.DataFrame()
err_val = '-'

for k,v in df_dict.items():  
  try:
    max_date = max(df_dict[k]['Date'])      
  except:
    max_date = err_val
  
  try:
    min_date = min(df_dict[k]['Date'])      
  except:
    min_date = err_val

  try:
    max_close = float(max(df_dict[k]['Close']))
  except:
    max_close = err_val
  
  try:
    min_close = float(min(df_dict[k]['Close']))
  except:
    min_close = err_val
  
  try:
    close_current = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['current'])]['Close'])
  except:
    close_current = err_val

  try:
    close_one_day = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['one_day'])]['Close'])
  except: 
    close_one_day = err_val
  
  try:
    close_three_days = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['three_days'])]['Close'])
  except: 
    close_three_days = err_val
  
  try:
    close_one_week = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['one_week'])]['Close'])
  except: 
    close_one_week = err_val
  
  try:
    close_one_month = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['one_month'])]['Close'])
  except: 
    close_one_month = err_val
  
  try:
    close_three_months = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['three_months'])]['Close'])
  except: 
    close_three_months = err_val
  
  try: 
    close_one_year = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['one_year'])]['Close'])
  except:  
    close_one_year = err_val
  
  try: 
    close_three_years = float(df_dict[k][(df_dict[k].Date == dates_dict_cleaned['three_yrs'])]['Close'])
  except:  
    close_three_years = err_val

# Define stock metrics
  try:
    one_day_return = round((close_current - close_one_day)/close_one_day*100,2)
  except: 
    one_day_return = err_val

  try:
    three_day_return = round((close_current - close_three_days)/close_three_days*100,2)
  except: 
    three_day_return = err_val    

  try:
    week_return = round((close_current - close_one_week)/close_one_week*100,2)
  except: 
    week_return = err_val    

  try:
    month_return = round((close_current - close_one_month)/close_one_month*100,2)
  except: 
    month_return = err_val    

  try:
    year_return = round((close_current - close_one_year)/close_one_year*100,2)
  except: 
    year_return = err_val    

  try:
    three_year_return = round((close_current - close_three_years)/close_three_years*100,2)
  except: 
    three_year_return = err_val

  try: 
    three_month_return = round((close_current - close_three_months)/close_three_months*100,2)
  except: 
    three_month_return = err_val    

  try:  
    month_year_ratio = round(three_month_return / three_year_return,2)
  except: 
    month_year_ratio = err_val

# Run tests
  ret_vals=[5,10,15,20,25]

  test_dict = {'new_max':[close_current >  max_close, 'current_close greater than max_close', 'highs_lows', close_current],
               'new_low':[close_current < min_close, 'current_close lower than min_close',   'highs_lows', close_current],
               '1_day_%':[abs(one_day_return) > ret_vals[0], '1_day_return_%% greater than %s' % (ret_vals[0]),   'rapid_change', one_day_return],                            
               '3_day_%':[abs(three_day_return) > ret_vals[1], '3_day_return_%% greater than %s' % (ret_vals[1]),   'rapid_change', three_day_return],
               'week_%':[abs(week_return) > ret_vals[2], 'week_return_%% greater than %s' % (ret_vals[2]),   'rapid_change', week_return],
               'month_%':[abs(month_return) > ret_vals[3], 'month_return_%% greater than %s' % (ret_vals[3]),   'rapid_change', month_return],
               '3_month_%':[abs(three_month_return) > ret_vals[4], '3_month_return_%% greater than %s' % (ret_vals[4]),   'rapid_change', three_month_return]
                }

  for key in test_dict.keys():
    try:
      if test_dict[key][0]:
        df_alerts = df_alerts.append({'stock': k, 'alert': key, 'description': test_dict[key][1], 'value': test_dict[key][3]},ignore_index=True)
      else:        
        pass
    except: 
      pass
       
  monitor_alert = 'NA'
  try: 
    if len(df_alerts['alert'][(df_alerts['stock']==k)]) > 0:
      monitor_alert = pd.Series(df_alerts['description'][(df_alerts['stock']==k)]).str.cat(sep='; ')      
    else: 
      pass
  except:
    pass    
    
  df_agg = df_agg.append({'stock' : k, 'close_price': close_current, 'max_close': max_close, 'min_close': min_close, '1_day_return_%': one_day_return, '3_day_return_%': three_day_return, '7_day_return_%': week_return, 'month_return_%': month_return, '3_month_return_%': three_month_return,'1_year_return': year_return, '3_year_return_%' : three_year_return, '3_month_return_%': three_month_return, 'monitor_alert': monitor_alert, 'max_date': max_date, 'min_date': min_date},ignore_index=True)

# Tidy DFs agg
df_agg = df_agg.sort_values(by=['monitor_alert'], ascending=True)
df_agg2 = df_agg[["stock","monitor_alert","close_price","max_close","min_close","1_day_return_%","3_day_return_%","7_day_return_%","month_return_%","3_month_return_%","1_year_return","3_year_return_%","min_date",]]
error_list_df = error_list_df.sort_values(by=['error_type','stock'])

try:
  df_alerts = df_alerts[["stock","alert","description","value"]]
except: 
  print('df_alerts does not exist')

# Create Excel file
file_name = 'Daily_Stock_Summary_'+ pd.to_datetime('today').strftime('%Y%m%d')  +'.xlsx'
writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

# Add Aggregate dataframe as sheet
df_agg2.to_excel(writer, sheet_name='Stock Summary', index=False)

# Add raw prices from df_dict 
for k,v in df_dict.items(): 
    v.to_excel(writer, sheet_name=str(k), index=False)

error_list_df.to_excel(writer, sheet_name=str('Stock Errors'), index=False)

df_alerts.to_excel(writer, sheet_name=str('Alarm Triggers'), index=False)
writer.save()

#Create email variables
stock_num = df_agg2.shape[0]  
row_num =  (stock_num * 25 * 365)+241
run_time = round(time.time() - start, 2)
alert_num = len(df_alerts)

try: 
  stocks = df_alerts['stock'].str.cat(sep=', ')
except:
  stocks = 'error'

daily_message = Mail(
    from_email= fromz,
    to_emails= to,
    subject='[Daily Report] Stock Summary - finance.watchdog',
        
    html_content=f"""      
    <p>Dear investor,</p>
    <p><strong>See attached Daily Stock Report!</strong><br /> No alerts triggered.</p>
    <p>Yours truly,<br /> finance.watchdog</p>
    
    <p>*****************************************</p>
    <table>
    <tbody>
    <tr>
    <td>Stocks analysed:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Alerts triggered:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Number of rows generated:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Run-time (seconds):</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Rule list:</td>
    <td><a href="https://docs.google.com/spreadsheets/d/1Ys-BSHbSgNjgGtaE--FLMDeuYelMdZfsVI1dJXufl9A/edit?usp=sharing">here</a></td>
    </tr>
    <tr>
    <td>Source code:</td>
    <td><a href="https://colab.research.google.com/drive/1pMUaGsLWICsshSuV-R63Ng9xWuQXq4kV">here</a></td>
    </tr>
    </tbody>
    </table>
    """ % (stock_num, alert_num, row_num, run_time))

## alert message
alert_message = Mail(
    from_email= fromz,
    to_emails= to,
    subject='[Alert triggered] Stock Summary - finance.watchdog',

    html_content=f"""      
    <p>Dear investor,</p>
    <p><strong>Alerts triggered on: % s</strong><br/> See attached Daily Stock Report!.</p>
    <p>Yours truly,<br /> finance.watchdog</p>
    <p>*****************************************<br/>
    *****************************************</p>
    <table>
    <tbody>
    <tr>
    <td>Stocks analysed:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Alerts triggered:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Number of rows generated:</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Run-time (seconds):</td>
    <td>% s</td>
    </tr>
    <tr>
    <td>Rule list:</td>
    <td><a href="rulelist.com">here</a></td>
    </tr>
    <tr>
    <td>Source code:</td>
    <td><a href="github.com">here</a></td>
    </tr>
    </tbody>
    </table>
    """ % (stocks, stock_num, alert_num, row_num, run_time))

# Run test 
if alert_num > 0: 
  message = alert_message
  print('is_alert')
else: 
  message = daily_message  
  print('is_daily')

# file encoding & attachment
with open('/content/'+file_name, 'rb') as f:
  data = f.read()
  f.close()
  encoded_file = base64.b64encode(data).decode()

attachedFile = Attachment(
  FileContent(encoded_file),
  FileName(file_name),
  FileType('application/vnd.ms-excel'),
  Disposition('attachment')
  )

message.attachment = attachedFile

# send email
try:
    sg = SendGridAPIClient(sendgrid_api_key)
    response = sg.send(message)
    print(response.status_code)    
except Exception as e:
    print('email cant send')
    print(e)
