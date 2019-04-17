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
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class AsyncRateLimitRepository implements RateLimitRepository<LocalRateLimit> {

    private LocalRateLimitRepository localCacheRateLimitRepository;
    private RateLimitRepository<RateLimit> remoteCacheRateLimitRepository;

    private final Set<String> keys = new HashSet<>();


    public void initialize() {
        Disposable subscribe = Observable
                .timer(5000, TimeUnit.MILLISECONDS, Schedulers.io())
                .repeat()
                .subscribe(tick -> refresh());

        // TODO: dispose subscribe when service is stopped
    }

    @Override
    public Single<LocalRateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        // Get data from local cache
        return localCacheRateLimitRepository
                .get(key)
                // There is no data for the key from local repository, let's create new local counter
                .switchIfEmpty(
                        Single.defer(() ->
                                // Local counter must be based on the latest value from the repository
                                remoteCacheRateLimitRepository
                                        .get(key)
                                        .map(LocalRateLimit::new)
                                        // If none, let's continue the process with a local counter
                                        .switchIfEmpty(Single.defer(() -> Single
                                                .just(supplier.get())
                                                .map(LocalRateLimit::new)))
                                        // In case of error when getting data from repository, fallback to a local counter
                                        .onErrorReturn(throwable -> new LocalRateLimit(supplier.get())))
                )
                // Once we get the counter, it's time to apply the weight
                .doOnSuccess(new Consumer<LocalRateLimit>() {
                    @Override
                    public void accept(LocalRateLimit localRateLimit) throws Exception {
                        // Increment local counter
                        localRateLimit.setLocal(localRateLimit.getLocal() + weight);

                        // We have to update the counter because the policy is based on this one
                        localRateLimit.setCounter(localRateLimit.getCounter() + weight);
                    }
                })
                // We are ok locally, update the local counter
                .flatMap(localRateLimit -> localCacheRateLimitRepository.save(localRateLimit))
                .doOnSuccess(new Consumer<LocalRateLimit>() {
                    @Override
                    public void accept(LocalRateLimit localRateLimit) throws Exception {
                        keys.add(localRateLimit.getKey());
                        /*
                        Observable
                                .timer(5000, TimeUnit.MILLISECONDS)
                                .map((Function<Long, RateLimit>) aLong -> localRateLimit)
//                                .flatMapSingle((Function<RateLimit, SingleSource<? extends RateLimit>>) rateLimit -> localCacheRateLimitRepository.get(rateLimit.getKey()).toSingle())
                                .subscribe(new Consumer<RateLimit>() {
                                    @Override
                                    public void accept(RateLimit rateLimit) throws Exception {
                                        keys.add(rateLimit.getKey());
                                    }
                                });
                                */
                    }
                });
    }

    private void refresh() {
        if ( !keys.isEmpty() ) {
            keys.forEach(new java.util.function.Consumer<String>() {
                @Override
                public void accept(String key) {
                    // Get the counter from local cache
                    localCacheRateLimitRepository
                            .get(key)
                            .toSingle()
                            .flatMap(new Function<LocalRateLimit, SingleSource<RateLimit>>() {
                                @Override
                                public SingleSource<RateLimit> apply(LocalRateLimit localRateLimit) throws Exception {
                                    return remoteCacheRateLimitRepository.incrementAndGet(key, localRateLimit.getLocal(), new Supplier<RateLimit>() {
                                        @Override
                                        public RateLimit get() {
                                            return localRateLimit;
                                        }
                                    });
                                }
                            })
                            .zipWith(
                                    localCacheRateLimitRepository.get(key).toSingle(),
                                    new BiFunction<RateLimit, LocalRateLimit, LocalRateLimit>() {
                                        @Override
                                        public LocalRateLimit apply(RateLimit rateLimit, LocalRateLimit localRateLimit) throws Exception {
                                            // Set the counter with the latest value from the repository
                                            localRateLimit.setCounter(rateLimit.getCounter());

                                            // Re-init the local counter
                                            localRateLimit.setLocal(0L);

                                            return localRateLimit;
                                        }
                                    })
                            // And save the new counter value into the local cache
                            .flatMap((Function<LocalRateLimit, SingleSource<? extends RateLimit>>) localRateLimit ->
                                    localCacheRateLimitRepository.save(localRateLimit))
                            .subscribe();
                }
            });

            // Clear keys
            keys.clear();
        }
    }

    @Override
    public Maybe<LocalRateLimit> get(String key) {
        throw new IllegalStateException();
    }

    @Override
    public Single<LocalRateLimit> save(LocalRateLimit rateLimit) {
        throw new IllegalStateException();
    }

    public void setLocalCacheRateLimitRepository(LocalRateLimitRepository localCacheRateLimitRepository) {
        this.localCacheRateLimitRepository = localCacheRateLimitRepository;
    }

    public void setRemoteCacheRateLimitRepository(RateLimitRepository remoteCacheRateLimitRepository) {
        this.remoteCacheRateLimitRepository = remoteCacheRateLimitRepository;
    }
}
