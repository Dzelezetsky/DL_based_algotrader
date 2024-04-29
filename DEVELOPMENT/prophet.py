from Additional_tools.additional_tools import all_data_scaller, sample_creator, to_sequences
from TSMIXER.TSMixer_models import TSMixer
from LSTM.LSTM_models import LSTMModel
from TRANSFORMERS.Transformer_models import Transformer_Encoder_Model, Transformer_EncoderDecoder_Model, PositionalEncoding


class Prophet():
	'''
	Class that makes prediction for an hour ahead:
	Input:
		data : pd.DataFrame
		companies : list
	Output:
		result_df : pd.DataFrame # predictions	
	'''
	def __init__(self, data, companies):
		'''
		Requires to choose a model that will predict future prices
		Input: choosed_model : string
			   data : pd.DataFrame
		'''
		self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
		self.data = data
		
		self.companies = ['AFKS', 'AFLT', 'AGRO', 'ALRS', 'CBOM', 'CHMF', 'ENPG', 'FEES', 'FIVE',
       					  'FIXP', 'GAZP', 'GLTR', 'GMKN', 'HYDR', 'IRAO', 'LKOH', 'MAGN', 'MGNT',
       					  'MOEX', 'MTSS', 'NLMK', 'NVTK', 'OZON', 'PHOR', 'PIKK', 'PLZL', 'POLY',
       					  'ROSN', 'RTKM', 'RUAL', 'SBER', 'SBERP', 'SGZH', 'SNGS', 'SNGSP',
       					  'TATN', 'TATNP', 'TCSG', 'TRNFP', 'VKCO', 'VTBR', 'YNDX']

		
		

	def predict_by_LSTM(self):

		self.model = LSTMModel(input_size=57, hidden_size=1024, num_layers=3, dropout=0.1, fcst_horizon=1).to(self.device)
		weights = weights_adoptation('WEIGHTS_BANK/LSTM')
		self.model.load_state_dict(weights)
		total_df = self.data.set_index('tradedate')
		
		
		predictions = []
		assets = []

		for company in companies:
			data = total_df[total_df.secid == company].drop('secid', 1).tail(15)
			__ , data, scaler_star = all_data_scaller(data, data)
			data = torch.Tensor(data).to(self.device)
			prediction = self.model(data)
			prediction = scaler_star.inverse_transform(prediction)
			predictions.append(prediction)
			assets.append(company)

		result_df = pd.DataFrame({'Assest':assets,'Predictions':predictions})

		return result_df
	

	def predict_by_Transformer_Enc(self):
		self.model = Transformer_Encoder_Model(input_dim=57, d_model=512, nhead=8, num_layers=1, dropout=0.1).to(self.device)
		weights = weights_adoptation('WEIGHTS_BANK/TRANS_ENC')
		self.model.load_state_dict(weights)
		total_df = self.data.set_index('tradedate')

		predictions = []
		assets = []

		for company in companies:
			data = total_df[total_df.secid == company].drop('secid', 1).tail(15)
			__ , data, scaler_star = all_data_scaller(data, data)
			data = torch.Tensor(data).to(self.device)
			prediction = self.model(data)
			prediction = scaler_star.inverse_transform(prediction)
			predictions.append(prediction)
			assets.append(company)

		result_df = pd.DataFrame({'Assest':assets,'Predictions':predictions})

		return result_df


	

	def predict_by_Transformer_Enc_Dec(self):
		self.model = Transformer_EncoderDecoder_Model(input_dim=57, d_model=512, nhead=8, num_layers=1, dropout=0.1).to(self.device)
		weights = weights_adoptation('WEIGHTS_BANK/TRANS_ENC_DEC')
		self.model.load_state_dict(weights)
		total_df = self.data.set_index('tradedate')

		predictions = []
		assets = []

		for company in companies:
			data = total_df[total_df.secid == company].drop('secid', 1).tail(15)
			__ , data, scaler_star = all_data_scaller(data, data)
			enc_input = torch.Tensor(data).to(self.device)
			dec_input = torch.Tensor(data[-1,0]).to(self.device)
			prediction = self.model( (enc_input, dec_input) )
			prediction = scaler_star.inverse_transform(prediction)
			predictions.append(prediction)
			assets.append(company)

		result_df = pd.DataFrame({'Assest':assets,'Predictions':predictions})

		return result_df




	def predict_by_TSMixer(self):	
		self.model = TSMixer(seq_len=15, num_features=57, forecast_horizon=1, dropout=0.1, num_of_blocks=1).to(self.device)
		weights = weights_adoptation('WEIGHTS_BANK/TSMIXER')
		self.model.load_state_dict(weights)
		total_df = self.data.set_index('tradedate')

		predictions = []
		assets = []

		for company in companies:
			data = total_df[total_df.secid == company].drop('secid', 1).tail(15)
			__ , data, scaler_star = all_data_scaller(data, data)
			data = torch.Tensor(data).to(self.device)
			prediction = self.model(data)
			prediction = scaler_star.inverse_transform(prediction)
			predictions.append(prediction)
			assets.append(company)

		result_df = pd.DataFrame({'Assest':assets,'Predictions':predictions})

		return result_df

		
	def weights_adoptation(path):
    	state_dict = torch.load(path)
    	new_state_dict = {}
    	for key in state_dict:
        	new_key = key.replace('module.','')
        	new_state_dict[new_key] = state_dict[key]
    	return new_state_dict

