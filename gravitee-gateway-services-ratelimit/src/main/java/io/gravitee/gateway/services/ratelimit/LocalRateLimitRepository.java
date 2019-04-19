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
import io.reactivex.SingleSource;
import io.reactivex.functions.Function;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class LocalRateLimitRepository implements RateLimitRepository<LocalRateLimit> {

    private final Logger LOGGER = LoggerFactory.getLogger(LocalRateLimitRepository.class);

    private final Cache cache;

    LocalRateLimitRepository(final Cache cache) {
        this.cache = cache;
    }

    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public Single<LocalRateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        lock.lock();

        return this.get(key)
                .map((Function<LocalRateLimit, RateLimit>) localRateLimit -> localRateLimit)
                .switchIfEmpty(Single.defer(() -> Single.just(supplier.get())))
                .flatMap(new Function<RateLimit, SingleSource<LocalRateLimit>>() {
                    @Override
                    public SingleSource<LocalRateLimit> apply(RateLimit rateLimit) throws Exception {
                        LocalRateLimit localRateLimit = (LocalRateLimit) rateLimit;

                        // Increment local counter
                        localRateLimit.setLocal(localRateLimit.getLocal() + weight);

                        // We have to update the counter because the policy is based on this one
                        localRateLimit.setCounter(localRateLimit.getCounter() + weight);

                        return save(localRateLimit);
                    }
                })
                .doOnSuccess(localRateLimit1 -> lock.unlock())
                .doOnError(throwable -> {
                    // Ensure that lock is released also on error
                    lock.unlock();
                });
    }

    @Override
    public Maybe<LocalRateLimit> get(String key) {
        LOGGER.debug("Retrieve rate-limiting for {} from {}", key, cache.getName());

        Element elt = cache.get(key);

        if (elt != null) {
            LocalRateLimit rateLimit = (LocalRateLimit) elt.getObjectValue();

            if (rateLimit.getResetTime() > System.currentTimeMillis()) {
                return Maybe.just(rateLimit);
            }

            cache.remove(key);
        }

        return Maybe.empty();
    }

    @Override
    public Single<LocalRateLimit> save(LocalRateLimit rate) {
        Element elt = cache.get(rate.getKey());
        if (elt != null) {
            return Single.just(rate);
        } else {
            saveInCache(rate);
            return Single.just(rate);
        }
    }

    private void saveInCache(LocalRateLimit rate) {
        long ttlInMillis = rate.getResetTime() - System.currentTimeMillis();
        int ttl = (int) (ttlInMillis / 1000L);
        if (ttl > 0) {
            LOGGER.debug("Put rate-limiting {} with a TTL {} into {}", rate, ttl, cache.getName());
            cache.put(new Element(rate.getKey(), rate, 0, ttl));
        } else {
            cache.put(new Element(rate.getKey(), rate, 0, 0));
        }
    }
}
