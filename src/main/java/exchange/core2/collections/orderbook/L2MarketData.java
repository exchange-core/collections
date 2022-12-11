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

import java.util.Arrays;

/**
 * L2 Market Data carrier object
 * <p>
 * NOTE: Can have dirty data, askSize and bidSize are important!
 */
public final class L2MarketData {

    public static final int L2_SIZE = 32;

    public int askSize;
    public int bidSize;

    public long[] askPrices;
    public long[] askVolumes;
    public long[] askOrders;
    public long[] bidPrices;
    public long[] bidVolumes;
    public long[] bidOrders;

    // when published
    public long timestamp;
    public long referenceSeq;

//    public long totalVolumeAsk;
//    public long totalVolumeBid;

    public L2MarketData(long[] askPrices, long[] askVolumes, long[] askOrders, long[] bidPrices, long[] bidVolumes, long[] bidOrders) {
        this.askPrices = askPrices;
        this.askVolumes = askVolumes;
        this.askOrders = askOrders;
        this.bidPrices = bidPrices;
        this.bidVolumes = bidVolumes;
        this.bidOrders = bidOrders;

        this.askSize = askPrices != null ? askPrices.length : 0;
        this.bidSize = bidPrices != null ? bidPrices.length : 0;
    }

    public L2MarketData(int askSize, int bidSize) {
        this.askPrices = new long[askSize];
        this.bidPrices = new long[bidSize];
        this.askVolumes = new long[askSize];
        this.bidVolumes = new long[bidSize];
        this.askOrders = new long[askSize];
        this.bidOrders = new long[bidSize];
    }

    public long[] getAskPricesCopy() {
        return Arrays.copyOf(askPrices, askSize);
    }

    public long[] getAskVolumesCopy() {
        return Arrays.copyOf(askVolumes, askSize);
    }

    public long[] getAskOrdersCopy() {
        return Arrays.copyOf(askOrders, askSize);
    }

    public long[] getBidPricesCopy() {
        return Arrays.copyOf(bidPrices, bidSize);
    }

    public long[] getBidVolumesCopy() {
        return Arrays.copyOf(bidVolumes, bidSize);
    }

    public long[] getBidOrdersCopy() {
        return Arrays.copyOf(bidOrders, bidSize);
    }

    public long totalOrderBookVolumeAsk() {
        long totalVolume = 0L;
        for (int i = 0; i < askSize; i++) {
            totalVolume += askVolumes[i];
        }
        return totalVolume;
    }

    public long totalOrderBookVolumeBid() {
        long totalVolume = 0L;
        for (int i = 0; i < bidSize; i++) {
            totalVolume += bidVolumes[i];
        }
        return totalVolume;
    }


    public L2MarketData copy() {
        return new L2MarketData(
                getAskPricesCopy(),
                getAskVolumesCopy(),
                getAskOrdersCopy(),
                getBidPricesCopy(),
                getBidVolumesCopy(),
                getBidOrdersCopy());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof L2MarketData)) {
            return false;
        }
        L2MarketData o = (L2MarketData) obj;

        if (askSize != o.askSize || bidSize != o.bidSize) {
            return false;
        }

        for (int i = 0; i < askSize; i++) {
            if (askPrices[i] != o.askPrices[i] || askVolumes[i] != o.askVolumes[i] || askOrders[i] != o.askOrders[i]) {
                return false;
            }
        }
        for (int i = 0; i < bidSize; i++) {
            if (bidPrices[i] != o.bidPrices[i] || bidVolumes[i] != o.bidVolumes[i] || bidOrders[i] != o.bidOrders[i]) {
                return false;
            }
        }
        return true;

    }

    // TODO hashcode
}
