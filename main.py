# // Finance Watch.Dog
# // Python3 script to query historical stock price data, test against custom rules and trigger notifications.

import yfinance as yf
import pandas as pd 
import xlsxwriter
import datetime as dt
import os
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
import time

start = time.time()

# Inputs 
tickers = ['MSFT', 'AAPL','GOOGL', 'BRK.B','JPM', 'V']
enable_report_functionality = 1 #set to 1 to enable Excel report build
enable_email_functionality = 0 #set to 1 to enable Email summary/notification (requires Sendgrid API key, freely available)

storage_loc = ''
from_email = 'your_email@mail.com'
to_email = 'recipient@mail.com'
sendgrid_key = 'XXXX'
response_txt = '\n >> '

# Find dates 
# // Available close_price based on DOW - no price info over weekend & 1 day data latency
if pd.to_datetime('today').strftime('%w') == '1': 
  current = (pd.to_datetime('today') - pd.Timedelta(days=3))
elif pd.to_datetime('today').strftime('%w') == '0':
  current = (pd.to_datetime('today') - pd.Timedelta(days=2))
else: 
  current = (pd.to_datetime('today') - pd.Timedelta(days=1))

dates_dict = {'three_yrs': current - pd.Timedelta(days=1095), 
              'one_year': current - pd.Timedelta(days=365), 
              'three_months': current - pd.Timedelta(days=90), 
              'one_month': current - pd.Timedelta(days=30), 
              'one_week': current - pd.Timedelta(days=7), 
              'three_days': current - pd.Timedelta(days=3), 
              'one_day': current - pd.Timedelta(days=1)
              }

dates_cleaned_dict = {'current': current.normalize()}

for k, v in dates_dict.items(): 
  if dates_dict[k].strftime('%w') == '0': 
    dates_cleaned_dict[k] = (dates_dict[k] - pd.Timedelta(days=2))
  elif dates_dict[k].strftime('%w') == '6': 
    dates_cleaned_dict[k] = (dates_dict[k] - pd.Timedelta(days=1))
  else: 
    dates_cleaned_dict[k] = dates_dict[k]
  dates_cleaned_dict[k] = dates_cleaned_dict[k].normalize()

# Get stock data
# // Create df_dict & df_error_list
df_dict = {}
df_error_list = pd.DataFrame(columns=['stock', 'error_type', 'value'])
print(response_txt,'Querying Yahoo Finance against ticker list...\n')
for i in tickers:
  stock = yf.Ticker(i)     
  df = pd.DataFrame(stock.history(period='max'))
  df = df.reset_index()
  if len(df) == 0:
    df_error_list = df_error_list.append({'stock': i, 'error_type': 'delisted', 'value': 'NA'},ignore_index=True)    
  elif max(pd.to_datetime(df['Date'],  infer_datetime_format=True)) < dates_cleaned_dict['current']:
    print('- '+ i + ':  Insufficient data - max_date=' + str(max(pd.to_datetime(df['Date'],  infer_datetime_format=True))))
    df_error_list = df_error_list.append({'stock': i, 'error_type': 'max_date', 'value': max(pd.to_datetime(df['Date'],  infer_datetime_format=True))},ignore_index=True)
  else: 
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date'],  infer_datetime_format=True)
    df = df.sort_values(by='Date', ascending=False)
    df_dict[i] = df

# Compute vars and metrics
# // Create df_agg
print(response_txt,'Running stock data against tests...')
df_agg = pd.DataFrame()
df_alerts = pd.DataFrame()

for k,v in df_dict.items():
  err_val = -10000

# Vars
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
    close_current = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['current'])]['Close'])
  except:
    close_current = err_val

  try:
    close_one_day = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['one_day'])]['Close'])
  except: 
    close_one_day = err_val

  try:
    close_three_days = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['three_days'])]['Close'])
  except: 
    close_three_days = err_val

  try:
    close_one_week = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['one_week'])]['Close'])
  except: 
    close_one_week = err_val

  try:
    close_one_month = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['one_month'])]['Close'])
  except: 
    close_one_month = err_val

  try:
    close_three_months = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['three_months'])]['Close'])
  except: 
    close_three_months = err_val

  try: 
    close_one_year = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['one_year'])]['Close'])
  except:  
    close_one_year = err_val

  try: 
    close_three_years = float(df_dict[k][(df_dict[k].Date == dates_cleaned_dict['three_yrs'])]['Close'])
  except:  
    close_three_years = err_val

# Metrics
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
  ret_vals=[3,5,8,12,15]

  test_dict = {'new_max':[close_current >  max_close, 'current_close greater than max_close', 'highs_lows', close_current],
               'new_low':[close_current < min_close, 'current_close lower than min_close',   'highs_lows', close_current],
               '1_day_%':[abs(one_day_return) > ret_vals[0], '1_day_return_%% greater than %s' % (ret_vals[0]),   'rapid_change', one_day_return],                            
               '3_day_%':[abs(three_day_return) > ret_vals[1], '3_day_return_%% greater than %s' % (ret_vals[1]),   'rapid_change', three_day_return],
               'week_%':[abs(week_return) > ret_vals[2], 'week_return_%% greater than %s' % (ret_vals[2]),   'rapid_change', week_return],
               'month_%':[abs(month_return) > ret_vals[3], 'month_return_%% greater than %s' % (ret_vals[3]),   'rapid_change', month_return],
               '3_month_%':[abs(three_month_return) > ret_vals[4], '3_month_return_%% greater than %s' % (ret_vals[4]),   'rapid_change', three_month_return]
                }

  for key in test_dict.keys():
    if test_dict[key][0] and test_dict[key][3] != err_val :
      print('* Alert triggered: ',k,' - ', key)
      df_alerts = df_alerts.append({'stock': k, 'alert': key, 'description': test_dict[key][1], 'value': test_dict[key][3]},ignore_index=True)
    elif test_dict[key][3] == err_val :             
      print('* ',k,': Unable to compute metric - ',key)
    else:
      pass

  monitor_alert = 'NA'
  try: 
    if len(df_alerts['alert'][(df_alerts['stock']==k)]) > 0:
      monitor_alert = pd.Series(df_alerts['description'][(df_alerts['stock']==k)]).str.cat(sep='; ')      
    else: 
      pass
  except:
    pass    

  df_agg = df_agg.append({'stock' : k, 
                          'close_price': close_current, 
                          'max_close': max_close, 
                          'min_close': min_close, 
                          '1_day_return_%': one_day_return, 
                          '3_day_return_%': three_day_return, 
                          '7_day_return_%': week_return, 
                          'month_return_%': month_return, 
                          '3_month_return_%': three_month_return,
                          '1_year_return': year_return, 
                          '3_year_return_%' : three_year_return, 
                          'monitor_alert': monitor_alert, 
                          'max_date': max_date, 
                          'min_date': min_date},ignore_index=True)

# Tidy DFs 
df_agg = df_agg.sort_values(by=['monitor_alert'], ascending=True)
df_agg = df_agg[["stock","monitor_alert","close_price","1_day_return_%","3_day_return_%","7_day_return_%","month_return_%","3_month_return_%","1_year_return","3_year_return_%","max_close","min_close","min_date",]]
df_error_list = df_error_list.sort_values(by=['error_type','stock'])

try:
  df_alerts = df_alerts[["stock","alert","description","value"]]
except: 
  pass
print(response_txt, 'Stocks queried: %i; Data retrieval errors: %i; Alerts triggered: %i' % (len(df_agg),len(df_error_list), len(df_alerts)))  
print(response_txt, 'Datasets built and available: df_agg, df_alerts, df_error_list & df_dict')  

# Create Excel file
if enable_report_functionality == 1:
  file_name = 'Daily_Stock_Summary_'+ pd.to_datetime('today').strftime('%Y%m%d')  +'.xlsx'
  file_extension = storage_loc + file_name
  print(response_txt,'Creating  excel report at: ',file_extension)  
  writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
  df_agg.to_excel(writer, sheet_name='Stock Summary', index=False)
  for k,v in df_dict.items(): 
      v.to_excel(writer, sheet_name=str(k), index=False)
  df_error_list.to_excel(writer, sheet_name=str('Stock Errors'), index=False)
  df_alerts.to_excel(writer, sheet_name=str('Alarm Triggers'), index=False)
  writer.save()
else: 
  pass

# Define email vars
if enable_email_functionality == 1:
  stock_num = len(df_agg)
  row_num =  (stock_num * 25 * 365)
  run_time = round(time.time() - start, 2)
  alert_num = len(df_alerts) 
  rule_list = 'https://docs.google.com/spreadsheets/d/1Ys-BSHbSgNjgGtaE--FLMDeuYelMdZfsVI1dJXufl9A/edit?usp=sharing'
  source_code = 'https://github.com/dleigh120/finance_watchdog'

  try: 
    stocks = str(df_alerts['stock'].unique()).replace(' ',',')[1:-1]
  except:
    stocks = 'error'

  # Push alert notification
  if alert_num > 0: 
    print(response_txt,'Sending  Alert email to:', to_email)  
    message = Mail(
      from_email= from_email,
      to_emails= to_email,
      subject='[Alert triggered] Stock Summary - finance.watchdog',
      html_content=f"""      
      <p>Dear investor,</p>
      <p><strong>Alerts triggered on: % s</strong><br/> See attached Daily Stock Report.</p>
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
      <td><a href="% s">here</a></td>
      </tr>
      <tr>
      <td>Source code:</td>
      <td><a href="% s">here</a></td>
      </tr>
      </tbody>
      </table>              
      """ % (stocks, stock_num, alert_num, row_num, run_time, rule_list, source_code)) 
  else: 
    print(response_txt, 'Sending  Daily Summary email to:', to_email)
    message = Mail(
      from_email= from_email,
      to_emails= to_email,
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
      <td><a href="% s">here</a></td>
      </tr>
      <tr>
      <td>Source code:</td>
      <td><a href="% s">here</a></td>
      </tr>
      </tbody>
      </table>
      """ % (stock_num, alert_num, row_num, run_time, rule_list, source_code))      
    
  # Encode attachment
  with open(file_extension, 'rb') as f:
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

  # Send Email
  try:
    sg = SendGridAPIClient(sendgrid_key)
    response = sg.send(message)
    print(response_txt,'Email successful, response code: ',response.status_code)    
  except Exception as e:
    print(response_txt,'Email failed: ',e)
else:
  pass
