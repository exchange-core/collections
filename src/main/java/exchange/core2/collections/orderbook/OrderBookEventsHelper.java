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
import org.agrona.MutableDirectBuffer;

public final class OrderBookEventsHelper {

    private final MutableDirectBuffer resultsBuffer;

    public OrderBookEventsHelper(MutableDirectBuffer resultsBuffer) {
        this.resultsBuffer = resultsBuffer;
    }

    public void sendTradeEvent(final IOrder matchingOrder,
                               final boolean makerCompleted,
                               final boolean takerCompleted,
                               final long size,
                               final long bidderHoldPrice) {

        int offset = resultsBuffer.getInt(IOrderBook.RESPONSE_CODE_SIZE);

        resultsBuffer.putByte(offset, IOrderBook.MATCHER_EVENT_TRADE);
        offset += BitUtil.SIZE_OF_BYTE;

        // TODO single write in the end of block
        resultsBuffer.putByte(offset, takerCompleted ? (byte) 1 : 0);
        offset += BitUtil.SIZE_OF_BYTE;

        // maker completed
        resultsBuffer.putByte(offset, makerCompleted ? (byte) 1 : 0);
        offset += BitUtil.SIZE_OF_BYTE;

        // matched order Id
        resultsBuffer.putLong(offset, matchingOrder.getOrderId());
        offset += BitUtil.SIZE_OF_LONG;

        // matched order UID
        resultsBuffer.putLong(offset, matchingOrder.getUid());
        offset += BitUtil.SIZE_OF_LONG;

        // matching size
        resultsBuffer.putLong(offset, size);
        offset += BitUtil.SIZE_OF_LONG;

        // matching price
        resultsBuffer.putLong(offset, matchingOrder.getPrice());
        offset += BitUtil.SIZE_OF_LONG;

        // matching order reserved price for released Exchange Bids funds
        resultsBuffer.putLong(offset, bidderHoldPrice);
        offset += BitUtil.SIZE_OF_LONG;

        resultsBuffer.putInt(IOrderBook.RESPONSE_CODE_SIZE, offset);
    }

    public void sendReduceEvent(final IOrder order,
                                final long reduceSize,
                                final boolean completed) {

        int offset = resultsBuffer.getInt(IOrderBook.RESPONSE_CODE_SIZE);

        resultsBuffer.putByte(offset, IOrderBook.MATCHER_EVENT_REDUCE);
        offset += BitUtil.SIZE_OF_BYTE;

        resultsBuffer.putByte(offset, completed ? (byte) 1 : 0);
        offset += BitUtil.SIZE_OF_BYTE;

        // fill action fields (for events handling)
        resultsBuffer.putByte(offset, order.getAction() == OrderAction.BID ? (byte) 1 : 0);
        offset += BitUtil.SIZE_OF_BYTE;

        // reduced/cancelled size
        resultsBuffer.putLong(offset, reduceSize);
        offset += BitUtil.SIZE_OF_LONG;

        // order price
        resultsBuffer.putLong(offset, order.getPrice());
        offset += BitUtil.SIZE_OF_LONG;

        // TODO can we use single price field ?
        // reserve price
        resultsBuffer.putLong(offset, order.getReserveBidPrice());
        offset += BitUtil.SIZE_OF_LONG;

        resultsBuffer.putInt(IOrderBook.RESPONSE_CODE_SIZE, offset);
    }

    // TODO can use common method REJECT/REDUCE_ASK/REDUCE_BID/REDUCE_ASK_COMPLETE/REDUCE_BID_COMPLETE ?

    public void attachRejectEvent(final long price,
                                  final long bidderHoldPrice,
                                  final long rejectedSize) {


        int offset = resultsBuffer.getInt(IOrderBook.RESPONSE_CODE_SIZE);

        resultsBuffer.putByte(offset, IOrderBook.MATCHER_EVENT_REJECT);
        offset += BitUtil.SIZE_OF_BYTE;

        // rejected size
        resultsBuffer.putLong(offset, rejectedSize);
        offset += BitUtil.SIZE_OF_LONG;

        // order price
        resultsBuffer.putLong(offset, price);
        offset += BitUtil.SIZE_OF_LONG;

        // TODO can we use single price field ?
        // reserve price
        // set command reserved price for correct released EBids
        resultsBuffer.putLong(offset, bidderHoldPrice);
        offset += BitUtil.SIZE_OF_LONG;

        resultsBuffer.putInt(IOrderBook.RESPONSE_CODE_SIZE, offset);
    }

}
