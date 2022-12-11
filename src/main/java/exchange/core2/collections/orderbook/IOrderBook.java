/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.collections.orderbook;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public interface IOrderBook<S extends ISymbolSpecification> extends StateHash {

    /**
     * Process new order.
     * Depending on price specified (whether the order is marketable),
     * order will be matched to existing opposite GTC orders from the order book.
     * In case of remaining volume (order was not matched completely):
     * IOC - reject it as partially filled.
     * GTC - place as a new limit order into th order book.
     * <p>
     * Rejection chain attached in case of error (to simplify risk handling)
     * <p>
     * |---byte orderCommandType---|---long uid---|---long orderId---|---long price---|
     * |---long size---|---byte orderAction---|---byte orderType---|---int productCode---|
     */
    void newOrder(DirectBuffer buffer, int offset);

    /**
     * Cancel order completely.
     * <p>
     * fills cmd.action  with original original order action
     * <p>
     * |---byte orderCommandType---|---long uid---|---long orderId---|---int productCode---|
     */
    void cancelOrder(DirectBuffer buffer, int offset);

    /**
     * Decrease the size of the order by specific number of lots
     * <p>
     * fills cmd.action  with original  order action
     * <p>
     * |---byte orderCommandType---|---long uid---|---long orderId---|---long reduceSize---|
     * |---int productCode---|
     */
    void reduceOrder(DirectBuffer buffer, int offset);

    /**
     * Move order
     * <p>
     * newPrice - new price (if 0 or same - order will not moved)
     * fills cmd.action  with original original order action
     * <p>
     * |---byte orderCommandType---|---long uid---|---long orderId---|---long newPrice---|
     * |---int productCode---|
     */
    void moveOrder(DirectBuffer buffer, int offset);

    // testing only ?
    int getOrdersNum(OrderAction action);

    // testing only ?
    long getTotalOrdersVolume(OrderAction action);

    // testing only ?
    IOrder getOrderById(long orderId);

    // testing only - validateInternalState without changing state
    void validateInternalState();

    /**
     * Search for all orders for specified user.<p>
     * Slow, because order book do not maintain uid-to-order index.<p>
     * Produces garbage.<p>
     * Orders must be processed before doing any other mutable call.<p>
     *
     * @param uid user id
     * @return list of orders
     */
    List<IOrder> findUserOrders(long uid);

    S getSymbolSpec();

    Stream<? extends IOrder> askOrdersStream(boolean sorted);

    Stream<? extends IOrder> bidOrdersStream(boolean sorted);

    /**
     * State hash for order books is implementation-agnostic
     * Look {@link IOrderBook#validateInternalState} for full internal state validation for de-serialized objects
     *
     * @return state hash code
     */
    @Override
    default int stateHash() {

        // log.debug("State hash of {}", orderBook.getClass().getSimpleName());
        // log.debug("  Ask orders stream: {}", orderBook.askOrdersStream(true).collect(Collectors.toList()));
        // log.debug("  Ask orders hash: {}", stateHashStream(orderBook.askOrdersStream(true)));
        // log.debug("  Bid orders stream: {}", orderBook.bidOrdersStream(true).collect(Collectors.toList()));
        // log.debug("  Bid orders hash: {}", stateHashStream(orderBook.bidOrdersStream(true)));
        // log.debug("  getSymbolSpec: {}", orderBook.getSymbolSpec());
        // log.debug("  getSymbolSpec hash: {}", orderBook.getSymbolSpec().stateHash());

        return Objects.hash(
                stateHashStream(askOrdersStream(true)),
                stateHashStream(bidOrdersStream(true)),
                getSymbolSpec().stateHash());
    }

     static int stateHashStream(final Stream<? extends StateHash> stream) {
        int h = 0;
        final Iterator<? extends StateHash> iterator = stream.iterator();
        while (iterator.hasNext()) {
            h = h * 31 + iterator.next().stateHash();
        }
        return h;
    }

    /**
     * Obtain current L2 Market Data snapshot
     *
     * @param size max size for each part (ask, bid)
     * @return L2 Market Data snapshot
     */
    default L2MarketData getL2MarketDataSnapshot(final int size) {
        final int asksSize = getTotalAskBuckets(size);
        final int bidsSize = getTotalBidBuckets(size);
        final L2MarketData data = new L2MarketData(asksSize, bidsSize);
        fillAsks(asksSize, data);
        fillBids(bidsSize, data);
        return data;
    }

    default L2MarketData getL2MarketDataSnapshot() {
        return getL2MarketDataSnapshot(Integer.MAX_VALUE);
    }

    /**
     * Request to publish L2 market data into outgoing disruptor message
     *
     * @param data - pre-allocated object from ring buffer
     */
    default void publishL2MarketDataSnapshot(L2MarketData data) {
        int size = L2MarketData.L2_SIZE;
        fillAsks(size, data);
        fillBids(size, data);
    }

    void fillAsks(int size, L2MarketData data);

    void fillBids(int size, L2MarketData data);

    int getTotalAskBuckets(int limit);

    int getTotalBidBuckets(int limit);


    /*
     * Error code
     */

    short MATCHING_SUCCESS = 0;

    short MATCHING_UNKNOWN_ORDER_ID = -3002;
    short MATCHING_UNSUPPORTED_COMMAND = -3004;
    short MATCHING_INVALID_ORDER_BOOK_ID = -3005;
    short MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT = -3041;
    short MATCHING_REDUCE_FAILED_WRONG_SIZE = -3051;

    /*
     * Incoming message offsets
     *
     */
    int PLACE_OFFSET_UID = 0;
    int PLACE_OFFSET_ORDER_ID = PLACE_OFFSET_UID + BitUtil.SIZE_OF_LONG;
    int PLACE_OFFSET_PRICE = PLACE_OFFSET_ORDER_ID + BitUtil.SIZE_OF_LONG;
    int PLACE_OFFSET_RESERVED_BID_PRICE = PLACE_OFFSET_PRICE + BitUtil.SIZE_OF_LONG;
    int PLACE_OFFSET_SIZE = PLACE_OFFSET_PRICE + BitUtil.SIZE_OF_LONG;
    int PLACE_OFFSET_ACTION = PLACE_OFFSET_SIZE + BitUtil.SIZE_OF_LONG;
    int PLACE_OFFSET_TYPE = PLACE_OFFSET_ACTION + BitUtil.SIZE_OF_BYTE;
    int PLACE_OFFSET_END = PLACE_OFFSET_TYPE + BitUtil.SIZE_OF_BYTE;

    int CANCEL_OFFSET_UID = 0;
    int CANCEL_OFFSET_ORDER_ID = CANCEL_OFFSET_UID + BitUtil.SIZE_OF_LONG;
    int CANCEL_OFFSET_END = CANCEL_OFFSET_ORDER_ID + BitUtil.SIZE_OF_LONG;

    int REDUCE_OFFSET_UID = 0;
    int REDUCE_OFFSET_ORDER_ID  = REDUCE_OFFSET_UID + BitUtil.SIZE_OF_LONG;
    int REDUCE_OFFSET_SIZE = REDUCE_OFFSET_ORDER_ID + BitUtil.SIZE_OF_LONG;
    int REDUCE_OFFSET_END = REDUCE_OFFSET_SIZE + BitUtil.SIZE_OF_LONG;

    int MOVE_OFFSET_UID = 0;
    int MOVE_OFFSET_ORDER_ID = MOVE_OFFSET_UID + BitUtil.SIZE_OF_LONG;
    int MOVE_OFFSET_PRICE = MOVE_OFFSET_ORDER_ID + BitUtil.SIZE_OF_LONG;
    int MOVE_OFFSET_END = MOVE_OFFSET_PRICE + BitUtil.SIZE_OF_LONG;

    /*
     * Outgoing message offset
     */
    int RESPONSE_CODE_SIZE = BitUtil.SIZE_OF_SHORT;
    int RESPONSE_FIRST_MSG_OFFSET = RESPONSE_CODE_SIZE + BitUtil.SIZE_OF_INT;


    /*
     * Order types
     */

    byte ORDER_TYPE_GTC = 0; // Good till Cancel - equivalent to regular limit order

    // Immediate or Cancel - equivalent to strict-risk market order
    byte ORDER_TYPE_IOC = 1; // with price cap
    byte ORDER_TYPE_IOC_BUDGET = 2; // with total amount cap

    // Fill or Kill - execute immediately completely or not at all
    byte ORDER_TYPE_FOK = 3; // with price cap
    byte ORDER_TYPE_FOK_BUDGET = 4; // total amount cap


    /*
     * Matcher Event types
     */

    // After cancel/reduce order - risk engine should unlock deposit accordingly
    byte MATCHER_EVENT_REDUCE = 0;

    // Trade event
    // Can be triggered by place ORDER or for MOVE order command.
    byte MATCHER_EVENT_TRADE = 1;

    // Reject event
    // Can happen only when MARKET order has to be rejected by Matcher Engine due lack of liquidity
    // That basically means no ASK (or BID) orders left in the order book for any price.
    // Before being rejected active order can be partially filled.
    byte MATCHER_EVENT_REJECT = 2;

    // Custom binary data attached
    byte MATCHER_EVENT_BINARY_EVENT = 3;


}
