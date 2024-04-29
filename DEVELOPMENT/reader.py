from moexalgo import Market, Ticker, session
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timedelta

class Reader():
	def __init__(self, login, password):

	'''
	Requires login and password to authorize in the stock market:

	Input: login : sting
		   password : string
	'''
		self.login = login
		self.password = password
		self.authorization = session.authorize(self.login, self.password)

		if self.authorization == True:
			if is_authorized:
       			print('Successfully authorized!')
    		else:
        		print('authorization problems!')
		
		self.col_list = ['secid', 'tradedate', 'tradetime', 'put_orders_b', 'put_orders_s',
       					 'put_val_b', 'put_val_s', 'put_vol_b', 'put_vol_s', 'put_vwap_b',
       					 'put_vwap_s', 'put_vol', 'put_val', 'put_orders', 'cancel_orders_b',
       					 'cancel_orders_s', 'cancel_val_b', 'cancel_val_s', 'cancel_vol_b',
       					 'cancel_vol_s', 'cancel_vwap_b', 'cancel_vwap_s', 'cancel_vol',
       					 'cancel_val', 'cancel_orders', 'pr_open', 'pr_high', 'pr_low',
       					 'pr_close', 'pr_std', 'vol', 'val', 'trades', 'pr_vwap', 'pr_change',
       					 'trades_b', 'trades_s', 'val_b_x', 'val_s_x', 'vol_b_x', 'vol_s_x',
       					 'disb', 'pr_vwap_b', 'pr_vwap_s', 'spread_bbo', 'spread_lv10',
       					 'spread_1mio', 'levels_b', 'levels_s', 'vol_b_y', 'vol_s_y', 'val_b_y',
       					 'val_s_y', 'imbalance_vol_bbo', 'imbalance_val_bbo', 'imbalance_vol',
       					 'imbalance_val', 'vwap_b', 'vwap_s', 'vwap_b_1mio', 'vwap_s_1mio']
    	self.full_list_of_companies = ['GAZP', 'SBER', 'LKOH', 'GMKN', 'MGNT',
       									'TATN', 'NVTK', 'SNGS', 'PLZL', 'SNGSP',
       									'PIKK', 'ROSN', 'SBERP', 'CHMF', 'NLMK',
       									'IRAO', 'YNDX', 'ALRS', 'RUAL', 'MTSS',
       									'MAGN', 'PHOR', 'RTKM', 'VTBR', 'TATNP',
       									'TCSG', 'AGRO', 'OZON', 'AFLT', 'AFKS',
      									'FEES', 'TRNFP', 'CBOM', 'VKCO', 'ENPG',
       									'SGZH', 'FIVE', 'POLY', 'MOEX', 'GLTR',
       									'HYDR', 'FIXP']

		self.evening_session =  ['19:10:00',' 19:15:00', '19:20:00', '19:25:00', '19:30:00',
       							  '19:35:00', '19:40:00', '19:45:00', '19:50:00', '19:55:00']
       							 							       																	        		

	def get_market_data(self, start_date, end_date, companies):
    	'''
		Gets market data and returns it in a ready to use format
		Input: start_date : sting
		       end_date : string
		       companies : list

		Output: market_data : pd.DataFrame
		   
		'''
		self.start_date = start_date
		self.end_date = end_date
		self.companies = companies


    	self.tradestats, self.orderstats, self.obstats = read(self.start_date, self.end_date, self.companies)
		self.data = concatinate(tradestats, orderstats, obstats, self.companies, self.col_list)
		self.market_data = aggregate(self.data)
    	
    	return self.market_data

    
    def  read(self, start_date, end_date, companies):
    	'''
		Method reads data and makes further preparation
		Input: start_date : sting
		       end_date : string
		       companies : list

		Output: tradestats : pd.DataFrame
				orderstats : pd.DataFrame
				obstats : pd.DataFrame

    	'''
    	dates = pd.date_range(
    				min(start_date, end_date),
    				max(start_date, end_date)
    				).strftime('%Y-%m-%d').tolist()
    	stocks = Market('stocks')

    	i = 1                         #Check is there any data for start_date. If not, shift data 
    	while stocks.tradestats(date=start_date).shape[0] == 0:
        	start_date = dates[i]
        	i += 1

        tradestats = stocks.tradestats(date=start_date)
    	tradestats = tradestats.loc[tradestats['ticker'].isin(companies)]
    	tradestats = tradestats.fillna(tradestats.mean())
    	tradestats.rename(columns={'ticker':'secid'}, inplace=True)

    	orderstats = stocks.orderstats(date=start_date)
    	orderstats = orderstats.loc[orderstats['ticker'].isin(companies)]
    	orderstats = orderstats.fillna(orderstats.mean())
    	orderstats.rename(columns={'ticker':'secid'}, inplace=True)

    	obstats = stocks.obstats(date=start_date)
    	obstats = obstats.loc[obstats['ticker'].isin(companies)]
    	obstats = obstats.fillna(obstats.mean())
    	obstats.rename(columns={'ticker':'secid'}, inplace=True)

    	for date in dates[1:]: # Run by dates and make further data preparations(fill Nans and rename columns)
        	tr = stocks.tradestats(date=date)
        	if tr.shape[0] == 0:
            	continue
        	tr = tr.loc[tr['ticker'].isin(companies)]
        	tr = tr.fillna(tr.mean())
        	tr.rename(columns={'ticker':'secid'}, inplace=True)
    
        	order = stocks.orderstats(date=date)
        	if order.shape[0] == 0:
        	    continue
        	order = order.loc[order['ticker'].isin(companies)]
        	order = order.fillna(order.mean())
        	order.rename(columns={'ticker':'secid'}, inplace=True)
    
        	ob = stocks.obstats(date=date)
        	if ob.shape[0] == 0:
        	    continue
        	ob = ob.loc[ob['ticker'].isin(companies)]
        	ob = ob.fillna(ob.mean())
        	ob.rename(columns={'ticker':'secid'}, inplace=True)
        
        	# Пополняем глобальные df вновь считанными и обработанными
        	tradestats = pd.concat([tradestats, tr])
        	orderstats = pd.concat([orderstats, order])
        	obstats = pd.concat([obstats, ob])	

        return tradestats, orderstats, obstats

    def concatinate(self, tradestats, orderstats, obstats, companies, col_list):
    	'''
    	Concatenate three dataframes into one using specific rules in order to prevent the loss of information

    	Input:  tradestats : pd.DataFrame
				orderstats : pd.DataFrame
				obstats : pd.DataFrame
		        companies : list
		        col_list : list

		Output: total_df : pd.DataFrame

    	'''	
    	total_df = pd.DataFrame(columns=col_list)

    	for company in companies:
    
        	tr = tradestats[tradestats.secid == company].drop('systime',1)
        	ob = obstats[obstats.secid == company].drop('systime',1)
        	order = orderstats[orderstats.secid == company].drop('systime',1)
        
        	if np.argmax([tr.shape[0], ob.shape[0], order.shape[0]]) == 0:
            	first_merge = tr.merge(ob, how='left', on=['secid','tradedate','tradetime'])
            	first_merge = first_merge.fillna(first_merge.mean())
            
            	second_merge = first_merge.merge(order, how='left', on=['secid','tradedate','tradetime'])
            	second_merge = second_merge.fillna(second_merge.mean())
            
            	total_df = pd.concat([total_df, second_merge])
        
       		elif np.argmax([tr.shape[0], ob.shape[0], order.shape[0]]) == 1:
            	first_merge = ob.merge(tr, how='left', on=['secid','tradedate','tradetime'])
            	first_merge = first_merge.fillna(first_merge.mean())
            
            	second_merge = first_merge.merge(order, how='left', on=['secid','tradedate','tradetime'])
            	second_merge = second_merge.fillna(second_merge.mean())
            
            	total_df = pd.concat([total_df, second_merge])
        
        	elif np.argmax([tr.shape[0], ob.shape[0], order.shape[0]]) == 2:
        		first_merge = order.merge(tr, how='left', on=['secid','tradedate','tradetime'])
            	first_merge = first_merge.fillna(first_merge.mean())
            
           	 	second_merge = first_merge.merge(ob, how='left', on=['secid','tradedate','tradetime'])
           		second_merge = second_merge.fillna(second_merge.mean())
            
            	total_df = pd.concat([total_df, second_merge])

        return total_df      	

    def aggregate(self, df):
    	'''
		Aggregates data from 5minute intervals to 1hour intervals

		Input : 
			df : pd.DataFrame
		Output :
			total_df : pd.DataFrame	
    	'''
    	self.columns = ['close', 'open', 'pr_change', 'spread_bbo', 'spread_lv10',
       					'spread_1mio', 'levels_b', 'levels_s', 'vol_b_x', 'vol_s_x', 'val_b_x',
       					'val_s_x', 'imbalance_vol_bbo', 'imbalance_val_bbo', 'imbalance_vol',
       					'imbalance_val', 'vwap_b', 'vwap_s', 'vwap_b_1mio', 'vwap_s_1mio',
       					'pr_high', 'pr_low', 'pr_std', 'vol', 'val', 'trades', 'pr_vwap',
       					'trades_b', 'trades_s', 'val_b_y', 'val_s_y', 'vol_b_y', 'vol_s_y',
       					'disb', 'pr_vwap_b', 'pr_vwap_s', 'put_orders_b', 'put_orders_s',
       					'put_val_b', 'put_val_s', 'put_vol_b', 'put_vol_s', 'put_vwap_b',
       					'put_vwap_s', 'put_vol', 'put_val', 'put_orders', 'cancel_orders_b',
       					'cancel_orders_s', 'cancel_val_b', 'cancel_val_s', 'cancel_vol_b',
       					'cancel_vol_s', 'cancel_vwap_b', 'cancel_vwap_s', 'cancel_vol',
       					'cancel_orders', 'secid', 'tradedate']
       	self.operations = {'spread_bbo':'mean',\
              'spread_lv10':'mean',\
              'spread_1mio':'mean',\
              'levels_b':'mean',\
              'levels_s':'mean',\
              'vol_b_x':'sum',\
              'vol_s_x':'sum',\
              'val_b_x':'sum',\
              'val_s_x':'sum',\
              'imbalance_vol_bbo':'mean',\
              'imbalance_val_bbo':'mean',\
              'imbalance_vol':'sum',\
              'imbalance_val':'sum',\
              'vwap_b':'mean',\
              'vwap_s':'mean',\
              'vwap_b_1mio':'mean',\
              'vwap_s_1mio':'mean',\
              'pr_high':'max',\
              'pr_low':'min',\
              'pr_std':'mean',\
              'vol':'sum',\
              'val':'sum',\
              'trades':'sum',\
              'pr_vwap':'mean',\
              'trades_b':'sum',\
              'trades_s':'sum',\
              'val_b_y':'sum',\
              'val_s_y':'sum',\
              'vol_b_y':'sum',\
              'vol_s_y':'sum',\
              'disb':'mean',\
              'pr_vwap_b':'mean',\
              'pr_vwap_s':'mean',\
              'put_orders_b':'sum',\
              'put_orders_s':'sum',\
              'put_val_b':'sum',\
              'put_val_s':'sum',\
              'put_vol_b':'sum',\
              'put_vol_s':'sum',\
              'put_vwap_b':'mean',\
              'put_vwap_s':'mean',\
              'put_vol':'sum',\
              'put_val':'sum',\
              'put_orders':'sum',\
              'cancel_orders_b':'sum',\
              'cancel_orders_s':'sum',\
              'cancel_val_b':'sum',\
              'cancel_val_s':'sum',\
              'cancel_vol_b':'sum',\
              'cancel_vol_s':'sum',\
              'cancel_vwap_b':'mean',\
              'cancel_vwap_s':'sum',\
              'cancel_vol':'sum',  
              'cancel_orders':'sum'}				
    	df['hour'] = 0
    	df = df[df.tradetime.isin(self.evening_session) == False ]
    	df = df.drop_duplicates()

    	# Создаём время торговой сессии
		start_time = datetime.strptime('10:05:00', '%H:%M:%S')
		end_time = datetime.strptime('18:40:00', '%H:%M:%S')
		step = timedelta(minutes=5)

		current_time = start_time
		trading_session = []

		while current_time <= end_time:
    		trading_session.append(current_time.strftime('%H:%M:%S'))
    		current_time += step
    	hour1 = trading_session[:12]
		hour2 = trading_session[12:24]
		hour3 = trading_session[24:36]
		hour4 = trading_session[36:48]
		hour5 = trading_session[48:60]
		hour6 = trading_session[60:72]
		hour7 = trading_session[72:84]
		hour8 = trading_session[84:96]
		hour9 = trading_session[96:]	

		df['hour'] = list(map(hours, df.tradetime))
		df.drop(['tradetime'],1, inplace=True)

		total_df = pd.DataFrame(columns=self.columns)

		companies = df.secid.unique()		
		for company in companies:
    		data = df[df['secid'] == company]
   			agg = data.groupby(['tradedate', 'hour']).aggregate(self.operations).reset_index()
    		agg.insert(loc = 0, column = 'open', value = data.groupby(['tradedate','hour']).pr_open.head(1).values )
    		agg.insert(loc = 0, column = 'close', value = data.groupby(['tradedate','hour']).pr_close.tail(1).values)
    		agg.insert(loc = 2, column = 'pr_change', 
    		                            value = (100 * (agg['close'] - agg['open']) / agg['open']).values )
    		agg['secid'] = company
    		agg.drop(['hour'],1,inplace=True)
    		total_df = pd.concat([total_df, agg])
		
		return total_df


	def hours(self, x):
		'''
		An auxiliary function for hours markup
		'''
    	if x in hour1:
    	    return 1
    	elif x in hour2:
    	    return 2
    	elif x in hour3:
    	    return 3
    	elif x in hour4:
    	    return 4
    	elif x in hour5:
    	    return 5
    	elif x in hour6:
    	    return 6
    	elif x in hour7:
    	    return 7
    	elif x in hour8:
    	    return 8
    	elif x in hour9:
    	    return 9		




