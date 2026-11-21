# Order book

[exchange collections](../README.md) › Order book

`collections-orderbook` contains the `IOrderBook` interface and a naive reference implementation
(`OrderBookNaiveImpl`), with the price levels in a `TreeMap`. Commands are passed as flat binary
records inside an Agrona `DirectBuffer`, so the hot path stays allocation-free:

```java
import exchange.core2.collections.orderbook.IOrderBook;

orderBook.newOrder(buffer, offset);     // place (and match) an order
orderBook.cancelOrder(buffer, offset);  // cancel completely
orderBook.reduceOrder(buffer, offset);  // decrease size by N lots
orderBook.moveOrder(buffer, offset);    // move to a new price
```

The exact command layouts are documented in the javadoc on `IOrderBook`.
