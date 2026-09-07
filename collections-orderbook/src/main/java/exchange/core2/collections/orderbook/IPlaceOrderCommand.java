package exchange.core2.collections.orderbook;

public interface IPlaceOrderCommand {

     long getOrderId(long offset);
     int getSymbolId(long offset);
     int get(long offset);

}
