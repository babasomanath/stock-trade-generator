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
import java.util.Date;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Captures the key elements of a stock trade, such as the ticker symbol, price,
 * number of shares, the type of the trade (buy or sell), and an id uniquely identifying
 * the trade.
 */
public class StockTrade {

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Represents the type of the stock trade eg buy or sell.
     */
    public enum TradeType {
        BUY,
        SELL
    }

    private String tickerSymbol;
    private TradeType tradeType;
    private double price;
    private long quantity;
    private long id;
    private Date date;
    
    public StockTrade() {
    }

    public StockTrade(String tickerSymbol, TradeType tradeType, double price, long quantity, long id) {
        this.tickerSymbol = tickerSymbol;
        this.tradeType = tradeType;
        this.price = price;
        this.quantity = quantity;
        this.id = id;
        this.date=new Date();
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
    
    public Date getDate(){
    	return date;
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
        return String.format("{id:%d,tradeType:\"%s\",quantity:%d,tickerSymbol:\"%s\",price:%.02f,date:\"%s\"}",
                id, tradeType, quantity, tickerSymbol, price,date);
    }

}
