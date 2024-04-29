import pandas as pd
import numpy as np
import cvxpy as cvx

class trader():
	'''
	Class that process trading signals(price predictions that come from the Prophet) and rebalance protfolio for the future hour.

	Input:
		price_predictions : pd.Series
		actual_prices : ps.Series
		transaction_costs : float
		SIGMA : np.array
		RISK_AVERSION : int/float

	Output:
		trading_vector : np.array
		expected_portfolio_return : float
	'''

	def rebalance(self, current_portfolio, pred_prices, actual_prices, transaction_costs, SIGMA, RISK_AVERSION):
		