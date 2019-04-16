/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.gateway.services.ratelimit;

import io.gravitee.repository.ratelimit.api.RateLimitRepository;
import io.gravitee.repository.ratelimit.model.RateLimit;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class AsyncRateLimitRepository implements RateLimitRepository {

    private LocalRateLimitRepository localCacheRateLimitRepository;
    private RateLimitRepository remoteCacheRateLimitRepository;
//    private RateLimitRepository aggregateCacheRateLimitRepository;
    private BlockingQueue<RateLimit> rateLimitsQueue;

    @Override
    public Single<RateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        return
                // Get data from local cache
                localCacheRateLimitRepository
                        .get(key)
                        .switchIfEmpty(
                                remoteCacheRateLimitRepository
                                        .get(key)
                                        .switchIfEmpty(Single.defer(() -> Single.just(supplier.get())))
                        )
                        .zipWith(
                                // Aggregate counter with data from aggregate cache
                                // Split the key to remove the gateway_id and get only the needed part
                                remoteCacheRateLimitRepository
                                        .get(key)
                                        .switchIfEmpty(Single.defer(() -> Single.just(supplier.get())))
                                , new BiFunction<RateLimit, RateLimit, RateLimit>() {
                                    @Override
                                    public RateLimit apply(RateLimit localRateLimit, RateLimit remoteRateLimit) throws Exception {
                                        localRateLimit.setCounter(localRateLimit.getCounter() + remoteRateLimit.getCounter());

                                        // TODO: save counter
                                        return localRateLimit;
                                    }
                                });
/*

        RateLimit aggregateRateLimit = aggregateCacheRateLimitRepository.get(parts[1]);
        if (aggregateRateLimit != null) {
            cachedRateLimit.setCounter(cachedRateLimit.getCounter() + aggregateRateLimit.getCounter());
            AggregateRateLimit extendRateLimit = new AggregateRateLimit(cachedRateLimit);
            extendRateLimit.setAggregateCounter(aggregateRateLimit.getCounter());
            return extendRateLimit;
        }

        return cachedRateLimit;
*/
    }

    private Single<LocalRateLimit> create(Supplier<RateLimit> supplier) {
        return Single
                .just(supplier.get())
                .map(new Function<RateLimit, LocalRateLimit>() {
                    @Override
                    public LocalRateLimit apply(RateLimit rateLimit) throws Exception {
                        return null;
                    }
                });
    }

    @Override
    public Maybe<RateLimit> get(String key) {
        throw new IllegalStateException();
    }

    /*
    private Single<RateLimit> getDistributedRateLimit(String key, Supplier<RateLimit> supplier) {

        String [] parts = KeySplitter.split(key);
        return ;
    }


    @Override
    public RateLimit get(String rateLimitKey) {
        // Get data from local cache
        RateLimit cachedRateLimit = localCacheRateLimitRepository.get(rateLimitKey);
        cachedRateLimit = (cachedRateLimit != null) ? cachedRateLimit : new RateLimit(rateLimitKey);

        // Aggregate counter with data from aggregate cache
        // Split the key to remove the gateway_id and get only the needed part
        String [] parts = KeySplitter.split(rateLimitKey);
        RateLimit aggregateRateLimit = aggregateCacheRateLimitRepository.get(parts[1]);
        if (aggregateRateLimit != null) {
            cachedRateLimit.setCounter(cachedRateLimit.getCounter() + aggregateRateLimit.getCounter());
            AggregateRateLimit extendRateLimit = new AggregateRateLimit(cachedRateLimit);
            extendRateLimit.setAggregateCounter(aggregateRateLimit.getCounter());
            return extendRateLimit;
        }

        return cachedRateLimit;
    }

    @Override
    public void save(RateLimit rateLimit) {
        if (rateLimit instanceof AggregateRateLimit) {
            AggregateRateLimit aggregateRateLimit = (AggregateRateLimit) rateLimit;
            aggregateRateLimit.setCounter(aggregateRateLimit.getCounter() - aggregateRateLimit.getAggregateCounter());
            aggregateRateLimit.setAggregateCounter(0L);
        }

        // Push data in local cache
        localCacheRateLimitRepository.save(rateLimit);

        // Push data in queue to store rate-limit asynchronously
        rateLimitsQueue.offer(rateLimit);
    }

    @Override
    public Iterator<RateLimit> findAsyncAfter(long timestamp) {
        throw new IllegalStateException();
    }
    */

    public void setLocalCacheRateLimitRepository(LocalRateLimitRepository localCacheRateLimitRepository) {
        this.localCacheRateLimitRepository = localCacheRateLimitRepository;
    }

    public RateLimitRepository getRemoteCacheRateLimitRepository() {
        return remoteCacheRateLimitRepository;
    }

    public void setRateLimitsQueue(BlockingQueue<RateLimit> rateLimitsQueue) {
        this.rateLimitsQueue = rateLimitsQueue;
    }
}
