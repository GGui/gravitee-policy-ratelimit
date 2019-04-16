package io.gravitee.gateway.services.ratelimit;

import io.gravitee.repository.ratelimit.model.RateLimit;

public class LocalRateLimit extends RateLimit {

    private long local;

    public LocalRateLimit(String key) {
        super(key);
    }

    public long getLocal() {
        return local;
    }

    public void setLocal(long local) {
        this.local = local;
    }
}
