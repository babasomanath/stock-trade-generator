/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.application.stocktrades.model;

import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Captures the key elements of a stock trade, such as the ticker symbol, price,
 * number of shares, the type of the trade (buy or sell), and an id uniquely
 * identifying the trade.
 */
public class StockTrade implements Serializable, Comparable<StockTrade> {

	/**
	 * DEFAULT SERIAL ID
	 */
	private static final long serialVersionUID = 1L;
	private final static ObjectMapper JSON = new ObjectMapper();
	static {
		JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Represents the type of the stock trade eg buy or sell.
	 */
	public enum TradeType {
		BUY, SELL
	}

	private String tickerSymbol;
	private TradeType tradeType;
	private double price;
	private long quantity;
	private long id;
	private Long timeInNanos;

	public StockTrade() {
	}

	public StockTrade(String tickerSymbol, TradeType tradeType, double price,
			long quantity, long id) {
		this.tickerSymbol = tickerSymbol;
		this.tradeType = tradeType;
		this.price = price;
		this.quantity = quantity;
		this.id = id;
		this.timeInNanos = System.nanoTime();
	}

	public String getTickerSymbol() {
		return tickerSymbol;
	}

	public TradeType getTradeType() {
		return tradeType;
	}

	public double getPrice() {
		return price;
	}

	public long getQuantity() {
		return quantity;
	}

	public long getId() {
		return id;
	}

	public Long getTimeInNanos() {
		return timeInNanos;
	}

	public byte[] toJsonAsBytes() {
		try {
			return JSON.writeValueAsBytes(this);
		} catch (Exception e) {
			return null;
		}
	}

	public static StockTrade fromJsonAsBytes(byte[] bytes) {
		try {
			return JSON.readValue(bytes, StockTrade.class);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }
    }

	@Override
	public int compareTo(StockTrade stockObject) {
		return this.timeInNanos.compareTo(stockObject.timeInNanos);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		long temp;
		temp = Double.doubleToLongBits(price);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (quantity ^ (quantity >>> 32));
		result = prime * result
				+ ((tickerSymbol == null) ? 0 : tickerSymbol.hashCode());
		result = prime * result
				+ ((timeInNanos == null) ? 0 : timeInNanos.hashCode());
		result = prime * result
				+ ((tradeType == null) ? 0 : tradeType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof StockTrade)) {
			return false;
		}
		StockTrade other = (StockTrade) obj;
		if (id != other.id) {
			return false;
		}
		if (Double.doubleToLongBits(price) != Double
				.doubleToLongBits(other.price)) {
			return false;
		}
		if (quantity != other.quantity) {
			return false;
		}
		if (tickerSymbol == null) {
			if (other.tickerSymbol != null) {
				return false;
			}
		} else if (!tickerSymbol.equals(other.tickerSymbol)) {
			return false;
		}
		if (timeInNanos == null) {
			if (other.timeInNanos != null) {
				return false;
			}
		} else if (!timeInNanos.equals(other.timeInNanos)) {
			return false;
		}
		if (tradeType != other.tradeType) {
			return false;
		}
		return true;
	}

}
