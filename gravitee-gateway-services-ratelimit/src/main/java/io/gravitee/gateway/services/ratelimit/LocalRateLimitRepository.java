package io.gravitee.gateway.services.ratelimit;

import io.gravitee.repository.ratelimit.api.RateLimitRepository;
import io.gravitee.repository.ratelimit.model.RateLimit;
import io.reactivex.Maybe;
import io.reactivex.Single;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class LocalRateLimitRepository implements RateLimitRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CachedRateLimitRepository.class);

    private final Cache cache;

    LocalRateLimitRepository(final Cache cache) {
        this.cache = cache;
    }

    @Override
    public Single<LocalRateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        throw new IllegalStateException();
    }

    @Override
    public Maybe<LocalRateLimit> get(String key) {
        LOGGER.debug("Retrieve rate-limiting for {} from {}", key, cache.getName());

        Element elt = cache.get(key);
        return (elt != null) ? Maybe.just((LocalRateLimit) elt.getObjectValue()) : Maybe.empty();
    }

    public Single<LocalRateLimit> save(LocalRateLimit rate) {
        long ttlInMillis = rate.getResetTime() - System.currentTimeMillis();
        if (ttlInMillis > 0L) {
            int ttl = (int) (ttlInMillis / 1000L);
            LOGGER.debug("Put rate-limiting {} with a TTL {} into {}", rate, ttl, cache.getName());
            cache.put(new Element(rate.getKey(), rate, 0,ttl));
            return Single.just(rate);
        }

        return Single.just(rate);
    }
}
