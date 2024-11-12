import yfinance as yf
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models, expected_returns
import yfinance as yf
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
from pypfopt import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns
from pypfopt import plotting
import copy
import numpy as np
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

# List of ticker symbols for assets
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'JPM', 'V', 'PG', 'TSLA', 'NVDA', 'MA', 'NFLX', 'UNH', 'PYPL', 'HD', 'DIS', 'BAC', 'INTC', 'CMCSA', 'ADBE']

# Add NASDAQ and S&P 500 to the list
tickers.extend(['^IXIC', '^GSPC'])

table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
sp500_tickers = table[0]['Symbol'].tolist()


# Download historical data
data = yf.download(sp500_tickers, start="2020-09-01", end="2023-12-08")['Adj Close']
data.dropna(axis=1, inplace=True)

# Calculate expected returns and sample covariance
mu = expected_returns.mean_historical_return(data)
S = risk_models.sample_cov(data)

# Optimize for maximum Sharpe ratio
ef = EfficientFrontier(mu, S)
weights = ef.max_sharpe(risk_free_rate=0.042)
cleaned_weights = ef.clean_weights()

print(cleaned_weights)
ef.portfolio_performance(verbose=True)

def plot_efficient_frontier_and_max_sharpe(mu, S):  
    # Optimize portfolio for maximal Sharpe ratio 
    ef = EfficientFrontier(mu, S)

    fig, ax = plt.subplots(figsize=(8,6))
    ef_max_sharpe = copy.deepcopy(ef)
    plotting.plot_efficient_frontier(ef, ax=ax, show_assets=False)

    # Find the max sharpe portfolio
    ef_max_sharpe.max_sharpe(risk_free_rate=0.02)
    ret_tangent, std_tangent, _ = ef_max_sharpe.portfolio_performance()
    ax.scatter(std_tangent, ret_tangent, marker="*", s=100, c="r", label="Max Sharpe")

    # Generate random portfolios
    n_samples = 1000
    w = np.random.dirichlet(np.ones(ef.n_assets), n_samples)
    rets = w.dot(ef.expected_returns)
    stds = np.sqrt(np.diag(w @ ef.cov_matrix @ w.T))
    sharpes = rets / stds
    ax.scatter(stds, rets, marker=".", c=sharpes, cmap="viridis_r")

    # Output
    ax.set_title("Efficient Frontier with Random Portfolios")
    ax.legend()
    plt.tight_layout()
    plt.show()
        
plot_efficient_frontier_and_max_sharpe(mu, S)


# Sort the cleaned weights to get the top N weighted stocks
top_n = 100  # Change this number to display a different count of top stocks
sorted_weights = sorted(cleaned_weights.items(), key=lambda x: x[1], reverse=True)[:top_n]

# Display the top N weighted stocks
for stock, weight in sorted_weights:
    print(f"{stock}: {weight * 100:.2f}%")


# Define the total investment amount
total_investment = 9000  # $1,000,000

# Calculate the allocation based on optimized weights
allocation = {stock: (weight * total_investment) for stock, weight in cleaned_weights.items()}

# Display the allocation for each stock in the portfolio
for stock, amount in allocation.items():
    if amount != 0:
        print(f"{stock}: ${amount:.2f}")