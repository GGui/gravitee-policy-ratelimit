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
import io.reactivex.Single;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
class CachedRateLimitRepository implements RateLimitRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CachedRateLimitRepository.class);

    private final Cache cache;

    CachedRateLimitRepository(final Cache cache) {
        this.cache = cache;
    }

    @Override
    public Single<RateLimit> incrementAndGet(String key, Supplier<RateLimit> supplier) {
        LOGGER.debug("Retrieve rate-limiting for {} from {}", key, cache.getName());

        Element elt = cache.get(key);
        return (elt != null) ? Single.just((RateLimit) elt.getObjectValue()) : Single.just(supplier.get());
    }
}
