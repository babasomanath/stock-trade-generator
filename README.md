# This is the KPL Stock Trade Generator

To run this code:

1) Perform a git clone:
 git clone https://github.com/babasomanath/stock-trade-generator/

2) Run the code:
```
cd stock-trade-generator
mvn assembly:assembly
java -cp target/StockTradeGenerator-1.0.0-complete.jar -Dstream-name=kinesis_start_3 -Dbackpressure-size=50000 -Dbackpressure-delay=500 com.amazonaws.services.kinesis.application.producer.Generator
```

To execute consistent traffic, use the "backpressure-size" and "backpressure-delay" settings
```
