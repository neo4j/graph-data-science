/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.api;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.progress.JobId;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;

public class EphemeralResultStore implements ResultStore {

    static final Duration CACHE_EVICTION_DURATION = Duration.of(10, ChronoUnit.MINUTES);

    private final Cache<JobId, ResultStoreEntry> resultEntries;

    public EphemeralResultStore() {
        var singleThreadScheduler = ExecutorServiceUtil.createSingleThreadScheduler("GDS-ResultStore");
        this.resultEntries = createCache(singleThreadScheduler);
    }

    @Override
    public void add(JobId jobId, ResultStoreEntry entry) {
        this.resultEntries.put(jobId, entry);
    }

    @Override
    @Nullable
    public ResultStoreEntry get(JobId jobId) {
        return this.resultEntries.getIfPresent(jobId);
    }

    @Override
    public boolean hasEntry(JobId jobId) {
        return this.resultEntries.getIfPresent(jobId) != null;
    }

    @Override
    public void remove(JobId jobId) {
        this.resultEntries.invalidate(jobId);
    }

    private static <K, V> Cache<K, V> createCache(ScheduledExecutorService singleThreadScheduler) {
        return Caffeine.newBuilder()
            .expireAfterAccess(CACHE_EVICTION_DURATION)
            .ticker(new ClockServiceWrappingTicker())
            .executor(singleThreadScheduler)
            .scheduler(Scheduler.forScheduledExecutorService(singleThreadScheduler))
            .build();
    }
    private static class ClockServiceWrappingTicker implements Ticker {
        @Override
        public long read() {
            return ClockService.clock().millis() * 1000000;
        }
    }
}
