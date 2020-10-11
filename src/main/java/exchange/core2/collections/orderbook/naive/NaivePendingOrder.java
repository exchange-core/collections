package exchange.core2.collections.orderbook.naive;

import exchange.core2.collections.orderbook.OrderAction;

public final class PendingOrder {


    public PendingOrder(long orderId, long price, long size, long filled, long reserveBidPrice, OrderAction action, long uid, long timestamp) {
        this.orderId = orderId;
        this.price = price;
        this.size = size;
        this.filled = filled;
        this.reserveBidPrice = reserveBidPrice;
        this.action = action;
        this.uid = uid;
        this.timestamp = timestamp;
    }

    final long orderId;

    final long price;

    final long size;

    final long filled;

    // new orders - reserved price for fast moves of GTC bid orders in exchange mode
    final long reserveBidPrice;

    // required for PLACE_ORDER only;
    final OrderAction action;

    final long uid;

    final long timestamp;

}
