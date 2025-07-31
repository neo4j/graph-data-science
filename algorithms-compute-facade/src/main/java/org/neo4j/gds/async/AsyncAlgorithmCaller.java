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
package org.neo4j.gds.async;

import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncAlgorithmCaller {

    private final ExecutorService workerPool;
    private final Log log;

    public AsyncAlgorithmCaller(ExecutorService workerPool, Log log) {
        this.workerPool = workerPool;
        this.log = log;
    }

    public <R> CompletableFuture<TimedAlgorithmResult<R>> run(
        AlgorithmCallable<R> algorithm,
        JobId jobId
    ) {
        return CompletableFuture
            .supplyAsync(
                () -> {
                    log.debug("Job: `%s` - Starting", jobId);
                    R result;
                    var computeMillis = new AtomicLong();
                    try(var ignored = ProgressTimer.start(computeMillis::set)) {
                        result = algorithm.call();
                    }
                    return new TimedAlgorithmResult<>(result, computeMillis.get());
                },
                workerPool
            ).whenComplete((r, e) -> {
                log.debug("Job: `%s` - Complete", jobId);
            });
    }
}
