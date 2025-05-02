# LSTM Model for Cryptocurrency Price Prediction

This repository contains an LSTM (Long Short-Term Memory) model implemented in Python using TensorFlow and Keras for predicting cryptocurrency prices. The model utilizes historical price data to forecast future price movements.

---

👉 **[View the idea (HTML)](https://reha-hazir.github.io/real-time-trading-data-analysis/analysis.html)**

---

## Overview

The LSTM model is a type of recurrent neural network (RNN) that is well-suited for sequence prediction tasks, such as time series forecasting. In this case, I use it to predict cryptocurrency prices based on historical data of various features.

## Dataset

The dataset used in this project consists of historical price data of cryptocurrencies, including:
- Open Price
- Close Price
- High Price
- Low Price
- Volume
- Number of Trades
- Technical Indicators (e.g., RSI, EMA, ROC)

## Model Architecture

The LSTM model architecture is designed as follows:

1. **Input Layer**: Sequential input of historical price and volume data.
2. **LSTM Layers**: Multiple LSTM layers to capture temporal dependencies in the data.
3. **Dense Layers**: Fully connected layers for further processing and prediction.
4. **Output Layer**: Single neuron output layer for predicting the next price (regression task).

## Stages of the Model

### 1. Data Preprocessing

- **Loading Data**: Read historical cryptocurrency price data.
- **Feature Engineering**: Compute additional features such as Relative Strength Index (RSI), Exponential Moving Average (EMA), etc.
- **Normalization**: Scale numerical features to a range suitable for neural networks (e.g., Min-Max scaling).

### 2. Building the LSTM Model

- **Define Architecture**: Configure the number of LSTM layers, units per layer, and activation functions.
- **Compile Model**: Specify loss function (e.g., Mean Squared Error for regression), optimizer (e.g., Adam), and evaluation metrics.
- **Train-Test Split**: Split the dataset into training and testing sets.

### 3. Training the Model

- **Fit Model**: Train the LSTM model on the training data.
- **Monitor Performance**: Track loss and metrics on both training and validation sets.
- **Early Stopping**: Implement early stopping to prevent overfitting.

### 4. Model Evaluation

- **Evaluate Performance**: Assess the model’s accuracy using metrics such as Mean Squared Error (MSE) on the test set.
- **Visualization**: Plot actual vs. predicted prices to visualize model performance.

### 5. Prediction

- **Generate Predictions**: Use the trained model to predict future cryptocurrency prices.
- **Deployment**: Deploy the model for real-time predictions or integrate it into a trading algorithm.

### 6.Results

- **Analysis**: Please check the last cell of data_analytics/analysis/analysis.ipynb file to see the line chart of the predictions.

---

👉 **[View the idea (HTML)](https://reha-hazir.github.io/real-time-trading-data-analysis/analysis.html)**

---
