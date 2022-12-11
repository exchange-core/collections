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
package exchange.core2.collections.orderbook.naive;

import exchange.core2.collections.orderbook.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class OrderBookNaiveImpl<S extends ISymbolSpecification> implements IOrderBook<S> {

    private static final Logger log = LoggerFactory.getLogger(OrderBookNaiveImpl.class);

    private final NavigableMap<Long, OrdersBucketNaive> askBuckets;
    private final NavigableMap<Long, OrdersBucketNaive> bidBuckets;

    private final S symbolSpec;

    private final LongObjectHashMap<NaivePendingOrder> idMap = new LongObjectHashMap<>();

    private final boolean logDebug;

    private final MutableDirectBuffer resultsBuffer;
    private final OrderBookEventsHelper eventsHelper;


    public OrderBookNaiveImpl(final S symbolSpec,
                              final boolean logDebug,
                              final MutableDirectBuffer resultsBuffer) {

        this.symbolSpec = symbolSpec;
        this.askBuckets = new TreeMap<>();
        this.bidBuckets = new TreeMap<>(Collections.reverseOrder());
        this.logDebug = logDebug;
        this.resultsBuffer = resultsBuffer;
        this.eventsHelper = new OrderBookEventsHelper(resultsBuffer);
    }


    @Override
    public void newOrder(final DirectBuffer buffer, final int offset) {

        final byte orderType = buffer.getByte(offset + PLACE_OFFSET_TYPE);

        initResponse(IOrderBook.MATCHING_SUCCESS);

        switch (orderType) {
            case ORDER_TYPE_GTC:
                newOrderPlaceGtc(buffer, offset);
                return;
            case ORDER_TYPE_IOC:
                newOrderMatchIoc(buffer, offset);
                return;
            case ORDER_TYPE_FOK_BUDGET:
                newOrderMatchFokBudget(buffer, offset);
                return;
            // TODO IOC_BUDGET and FOK support
        }

        log.warn("Unsupported order type: {}", orderType);
        final long price = buffer.getLong(offset + PLACE_OFFSET_PRICE);
        final long size = buffer.getLong(offset + PLACE_OFFSET_SIZE);
        eventsHelper.attachRejectEvent(price, price, size);
    }


    private void newOrderPlaceGtc(final DirectBuffer buffer, final int offset) {

        final OrderAction action = OrderAction.of(buffer.getByte(offset + PLACE_OFFSET_ACTION));
        final long price = buffer.getLong(offset + PLACE_OFFSET_PRICE);
        final long size = buffer.getLong(offset + PLACE_OFFSET_SIZE);
        final long reserveBidPrice = buffer.getLong(offset + PLACE_OFFSET_RESERVED_BID_PRICE);

        // check if order is marketable (if there are opposite matching orders)
        final SortedMap<Long, OrdersBucketNaive> subTree = subtreeForMatching(action, price);
        final long filledSize = tryMatchInstantly(size, reserveBidPrice, subTree, 0);

        if (filledSize == size) {
            // order was matched completely - nothing to place - can just return
            return;
        }

        final long newOrderId = buffer.getLong(offset + PLACE_OFFSET_ORDER_ID);
        if (idMap.containsKey(newOrderId)) {
            // duplicate order id - can match, but can not place

            eventsHelper.attachRejectEvent(price, price, size - filledSize);
            log.warn("duplicate order id: {}", newOrderId);
        }

        final long uid = buffer.getLong(offset + PLACE_OFFSET_UID);

        // normally placing regular GTC limit order
        final NaivePendingOrder orderRecord = new NaivePendingOrder(
                newOrderId,
                price,
                size,
                filledSize,
                reserveBidPrice,
                action,
                uid,
                0L); // TODO save timestamp

        getBucketsByAction(action)
                .computeIfAbsent(price, p -> new OrdersBucketNaive(p, eventsHelper))
                .put(orderRecord);

        idMap.put(newOrderId, orderRecord);
    }

    private void newOrderMatchIoc(final DirectBuffer buffer, final int offset) {

        final long price = buffer.getLong(offset + PLACE_OFFSET_PRICE);
        final long size = buffer.getLong(offset + PLACE_OFFSET_SIZE);
        final OrderAction action = OrderAction.of(buffer.getByte(offset + PLACE_OFFSET_ACTION));

        final SortedMap<Long, OrdersBucketNaive> subtree = subtreeForMatching(action, price);
        final long filledSize = tryMatchInstantly(size, price, subtree, 0);

        final long rejectedSize = size - filledSize;

        if (rejectedSize != 0) {
            // was not matched completely - send reject for not-completed IoC order
            eventsHelper.attachRejectEvent(price, price, rejectedSize);
        }
    }

    private void newOrderMatchFokBudget(final DirectBuffer buffer, final int offset) {

        final OrderAction action = OrderAction.of(buffer.getByte(offset + PLACE_OFFSET_ACTION));
        final long size = buffer.getLong(offset + PLACE_OFFSET_SIZE);

        final SortedMap<Long, OrdersBucketNaive> fullSubtree = action == OrderAction.ASK ? bidBuckets : askBuckets;

        final Optional<Long> budget = checkBudgetToFill(size, fullSubtree);

        final long price = buffer.getLong(offset + PLACE_OFFSET_PRICE);

        if (logDebug) log.debug("Budget calc: {} requested: {}", budget, price);

        if (budget.isPresent() && isBudgetLimitSatisfied(action, budget.get(), price)) {
            tryMatchInstantly(size, price, fullSubtree, 0);
        } else {
            eventsHelper.attachRejectEvent(price, price, size);
        }
    }

    private boolean isBudgetLimitSatisfied(final OrderAction orderAction, final long calculated, final long limit) {
        return calculated == limit || (orderAction == OrderAction.BID ^ calculated > limit);
    }


    private Optional<Long> checkBudgetToFill(
            long size,
            final SortedMap<Long, OrdersBucketNaive> matchingBuckets) {

        long budget = 0;

        for (final OrdersBucketNaive bucket : matchingBuckets.values()) {

            final long availableSize = bucket.getTotalVolume();
            final long price = bucket.getPrice();

            if (size > availableSize) {
                size -= availableSize;
                budget += availableSize * price;
                if (logDebug) log.debug("add    {} * {} -> {}", price, availableSize, budget);
            } else {
                final long result = budget + size * price;
                if (logDebug) log.debug("return {} * {} -> {}", price, size, result);
                return Optional.of(result);
            }
        }
        if (logDebug) log.debug("not enough liquidity to fill size={}", size);
        return Optional.empty();
    }

    private SortedMap<Long, OrdersBucketNaive> subtreeForMatching(final OrderAction action, final long price) {
        return (action == OrderAction.ASK ? bidBuckets : askBuckets)
                .headMap(price, true);
    }

    /**
     * Match the order instantly to specified sorted buckets map
     * Fully matching orders are removed from orderId index
     * Should any trades occur - they sent to tradesConsumer
     *
     * @param matchingBuckets - sorted buckets map
     * @param filled          - current 'filled' value for the order
     * @return new filled size
     */
    private long tryMatchInstantly(
            final long takerSize,
            final long reserveBidPriceTaker,
            final SortedMap<Long, OrdersBucketNaive> matchingBuckets,
            long filled) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        if (matchingBuckets.size() == 0) {
            return filled;
        }

        for (final OrdersBucketNaive bucket : matchingBuckets.values()) {

            final long sizeLeft = takerSize - filled;

            filled += bucket.match(
                    sizeLeft,
                    reserveBidPriceTaker,
                    idMap::remove);

            // remove empty buckets
            if (bucket.getTotalVolume() == 0) {
                matchingBuckets.remove(bucket.getPrice());
            }

            if (filled == takerSize) {
                // takerSize matched completely
                break;
            }
        }

        return filled;
    }


    @Override
    public void cancelOrder(DirectBuffer buffer, int offset) {

        final long orderId = buffer.getLong(offset + CANCEL_OFFSET_ORDER_ID);
        final long cmdUid = buffer.getLong(offset + CANCEL_OFFSET_UID);

        final NaivePendingOrder order = idMap.get(orderId);
        if (order == null || order.uid != cmdUid) {
            // order already matched and removed from order book previously
            initResponse(MATCHING_UNKNOWN_ORDER_ID);
            return;
        }

        // now can remove it
        idMap.remove(orderId);

        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final long price = order.price;
        final OrdersBucketNaive ordersBucket = buckets.get(price);

        // remove order and whole bucket if its empty
        ordersBucket.remove(orderId, cmdUid);
        if (ordersBucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        initResponse(MATCHING_SUCCESS);

        // send reduce event
        eventsHelper.sendReduceEvent(
                order,
                order.getSize() - order.getFilled(),
                true);
    }

    @Override
    public void reduceOrder(DirectBuffer buffer, int offset) {

        final long orderId = buffer.getLong(offset + REDUCE_OFFSET_ORDER_ID);
        final long requestedReduceSize = buffer.getLong(offset + REDUCE_OFFSET_SIZE);
        final long cmdUid = buffer.getLong(offset + REDUCE_OFFSET_UID);

        if (requestedReduceSize <= 0) {
            initResponse(MATCHING_REDUCE_FAILED_WRONG_SIZE);
            return;
        }

        final NaivePendingOrder order = idMap.get(orderId);
        if (order == null || order.uid != cmdUid) {
            // already matched, moved or cancelled
            initResponse(MATCHING_UNKNOWN_ORDER_ID);
            return;
        }

        final long remainingSize = order.size - order.filled;
        final long reduceBy = Math.min(remainingSize, requestedReduceSize);

        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final OrdersBucketNaive ordersBucket = buckets.get(order.price);

        final boolean canRemove = (reduceBy == remainingSize);

        if (canRemove) {

            // now can remove order
            idMap.remove(orderId);

            // canRemove order and whole bucket if it is empty
            ordersBucket.remove(orderId, cmdUid);
            if (ordersBucket.getTotalVolume() == 0) {
                buckets.remove(order.price);
            }

        } else {

            order.size -= reduceBy;
            ordersBucket.reduceSize(reduceBy);
        }

        initResponse(MATCHING_SUCCESS);

        // send reduce event
        eventsHelper.sendReduceEvent(
                order,
                reduceBy,
                canRemove);
    }

    @Override
    public void moveOrder(DirectBuffer buffer, int offset) {

        final long orderId = buffer.getLong(offset + MOVE_OFFSET_ORDER_ID);
        final long newPrice = buffer.getLong(offset + MOVE_OFFSET_PRICE);
        final long cmdUid = buffer.getLong(offset + MOVE_OFFSET_UID);

        final NaivePendingOrder order = idMap.get(orderId);
        if (order == null || order.uid != cmdUid) {
            // already matched, moved or cancelled
            initResponse(MATCHING_UNKNOWN_ORDER_ID);
            return;
        }

        // reserved price risk check for exchange bids
        if (order.action == OrderAction.BID && symbolSpec.isExchangeType() && newPrice > order.reserveBidPrice) {
            initResponse(MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT);
            return;
        }

        initResponse(MATCHING_SUCCESS);

        final long price = order.price;
        final NavigableMap<Long, OrdersBucketNaive> buckets = getBucketsByAction(order.action);
        final OrdersBucketNaive bucket = buckets.get(price);

        // take order out of the original bucket and clean bucket if its empty
        bucket.remove(orderId, cmdUid);

        if (bucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        order.price = newPrice;

        // try match with new price
        final long filled = tryMatchInstantly(
                order.size,
                order.reserveBidPrice,
                subtreeForMatching(order.action, newPrice),
                order.filled);

        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMap.remove(orderId);

        } else {
            order.filled = filled;

            // if not filled completely - put it into corresponding bucket
            buckets.computeIfAbsent(newPrice, p -> new OrdersBucketNaive(p, eventsHelper))
                    .put(order);
        }
    }

    /**
     * Get bucket by order action
     *
     * @param action - action
     * @return bucket - navigable map
     */
    private NavigableMap<Long, OrdersBucketNaive> getBucketsByAction(OrderAction action) {
        return action == OrderAction.ASK ? askBuckets : bidBuckets;
    }


    /**
     * Get order from internal map
     *
     * @param orderId - order Id
     * @return order from map
     */
    @Override
    public IOrder getOrderById(long orderId) {
        return idMap.get(orderId);
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        if (size == 0) {
            data.askSize = 0;
            return;
        }

        int i = 0;
        for (OrdersBucketNaive bucket : askBuckets.values()) {
            data.askPrices[i] = bucket.getPrice();
            data.askVolumes[i] = bucket.getTotalVolume();
            data.askOrders[i] = bucket.getNumOrders();
            if (++i == size) {
                break;
            }
        }
        data.askSize = i;
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {
        if (size == 0) {
            data.bidSize = 0;
            return;
        }

        int i = 0;
        for (OrdersBucketNaive bucket : bidBuckets.values()) {
            data.bidPrices[i] = bucket.getPrice();
            data.bidVolumes[i] = bucket.getTotalVolume();
            data.bidOrders[i] = bucket.getNumOrders();
            if (++i == size) {
                break;
            }
        }
        data.bidSize = i;
    }

    @Override
    public int getTotalAskBuckets(final int limit) {
        return Math.min(limit, askBuckets.size());
    }

    @Override
    public int getTotalBidBuckets(final int limit) {
        return Math.min(limit, bidBuckets.size());
    }

    @Override
    public void validateInternalState() {
        askBuckets.values().forEach(OrdersBucketNaive::validate);
        bidBuckets.values().forEach(OrdersBucketNaive::validate);
    }

    @Override
    public List<IOrder> findUserOrders(final long uid) {
        final List<IOrder> list = new ArrayList<>();
        final Consumer<OrdersBucketNaive> bucketConsumer =
                bucket -> bucket.forEachOrder(
                        order -> {
                            if (order.uid == uid) {
                                list.add(order);
                            }
                        });
        askBuckets.values().forEach(bucketConsumer);
        bidBuckets.values().forEach(bucketConsumer);
        return list;
    }

    @Override
    public S getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<IOrder> askOrdersStream(final boolean sorted) {
        return askBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    @Override
    public Stream<IOrder> bidOrdersStream(final boolean sorted) {
        return bidBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    // for testing only
    @Override
    public int getOrdersNum(OrderAction action) {
        final NavigableMap<Long, OrdersBucketNaive> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
//        int askOrders = askBuckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
//        int bidOrders = bidBuckets.values().stream().mapToInt(OrdersBucketNaive::getNumOrders).sum();
        //log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
//        int knownOrders = idMap.size();
//        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";
    }

    @Override
    public long getTotalOrdersVolume(OrderAction action) {
        final NavigableMap<Long, OrdersBucketNaive> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToLong(OrdersBucketNaive::getTotalVolume).sum();
    }


    private void initResponse(final short responseCode) {
        resultsBuffer.putShort(0, responseCode);
        resultsBuffer.putInt(IOrderBook.RESPONSE_CODE_SIZE, 0);
    }
}

