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
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.processors.PublishProcessor;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class AsyncRateLimitRepository implements RateLimitRepository<LocalRateLimit> {

    private LocalRateLimitRepository localCacheRateLimitRepository;
    private RateLimitRepository<RateLimit> remoteCacheRateLimitRepository;

    private final PublishProcessor<String> updater = PublishProcessor.create();



            public void initialize() {
                updater
                        .buffer(5000, TimeUnit.MILLISECONDS)
                        .subscribe(this::refresh);
            };

    @Override
    public Single<LocalRateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        // Get data from local cache
        return localCacheRateLimitRepository
                .get(key)
                .switchIfEmpty(
                        remoteCacheRateLimitRepository
                                .get(key)
                                .map(LocalRateLimit::new)
                                .switchIfEmpty(Single.defer(() -> Single
                                        .just(supplier.get())
                                        .map(LocalRateLimit::new)))
                )
                .doOnSuccess(new Consumer<LocalRateLimit>() {
                    @Override
                    public void accept(LocalRateLimit localRateLimit) throws Exception {
                        // Increment local counter
                        localRateLimit.setLocal(localRateLimit.getLocal() + weight);
                        localRateLimit.setCounter(localRateLimit.getCounter() + weight);
                    }
                })
                //TODO: implement in case of error with remote repository
                .flatMap(localRateLimit -> localCacheRateLimitRepository.save(localRateLimit))
                .doOnSuccess(new Consumer<LocalRateLimit>() {
                    @Override
                    public void accept(LocalRateLimit localRateLimit) throws Exception {
                        Observable
                                .timer(5000, TimeUnit.MILLISECONDS)
                                .map((Function<Long, RateLimit>) aLong -> localRateLimit)
                                .flatMapSingle((Function<RateLimit, SingleSource<? extends RateLimit>>) rateLimit -> localCacheRateLimitRepository.get(rateLimit.getKey()).toSingle())
                                .subscribe(new Consumer<RateLimit>() {
                                    @Override
                                    public void accept(RateLimit rateLimit) throws Exception {
                                        updater.onNext(rateLimit.getKey());
                                    }
                                });
                    }
                });
    }

    private void refresh(Collection<String> keys) {
        if (keys != null) {
            // Is there any way to avoid duplicate from publishprocessor ?
            new HashSet<>(keys).forEach(new java.util.function.Consumer<String>() {
                @Override
                public void accept(String key) {
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
                                    localRateLimit.setCounter(rateLimit.getCounter());
                                    return localRateLimit;
                                }
                            })
                            .flatMap(new Function<LocalRateLimit, SingleSource<? extends RateLimit>>() {
                                @Override
                                public SingleSource<? extends RateLimit> apply(LocalRateLimit localRateLimit) throws Exception {
                                    return localCacheRateLimitRepository.save(localRateLimit);
                                }
                            })
                    .subscribe();

                }
            });
        }
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

    public RateLimitRepository getRemoteCacheRateLimitRepository() {
        return remoteCacheRateLimitRepository;
    }
}
