/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.logging.Log;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class BatchingProgressLogger implements ProgressLogger {
    public static final long MAXIMUM_LOG_INTERVAL = (long) Math.pow(2, 13);

    private final Log log;
    private final int concurrency;
    private long taskVolume;
    private long batchSize;
    private final String task;
    private final LongAdder progressCounter;
    private final ThreadLocal<MutableLong> callCounter;

    private int globalPercentage;

    private static long calculateBatchSize(long taskVolume, int concurrency) {
        // target 100 logs per full run (every 1 percent)
        var batchSize = taskVolume / 100;
        // split batchSize into thread-local chunks
        batchSize /= concurrency;
        // batchSize needs to be a power of two
        return Math.max(1, BitUtil.nextHighestPowerOfTwo(batchSize));
    }

    public BatchingProgressLogger(Log log, long taskVolume, String task, int concurrency) {
        this(log, taskVolume, calculateBatchSize(taskVolume, concurrency), task, concurrency);
    }

    public BatchingProgressLogger(Log log, long taskVolume, long batchSize, String task, int concurrency) {
        this.log = log;
        this.taskVolume = taskVolume;
        this.batchSize = batchSize;
        this.task = task;

        this.progressCounter = new LongAdder();
        this.callCounter = ThreadLocal.withInitial(MutableLong::new);
        this.concurrency = concurrency;
        this.globalPercentage = -1;
    }

    @Override
    public void logProgress(Supplier<String> msgFactory) {
        progressCounter.increment();
        var localProgress = callCounter.get();
        if (localProgress.longValue() < batchSize && (localProgress.incrementAndGet() >= batchSize)) {
            doLogPercentage(msgFactory);
            localProgress.setValue(0L);
        }
    }

    @Override
    public void logProgress(long progress, Supplier<String> msgFactory) {
        if (progress == 0) {
            return;
        }
        progressCounter.add(progress);
        var localProgress = callCounter.get();
        if (localProgress.longValue() < batchSize && (localProgress.addAndGet(progress) >= batchSize)) {
            doLogPercentage(msgFactory);
            localProgress.setValue(localProgress.longValue() & (batchSize - 1));
        }
    }

    private synchronized void doLogPercentage(Supplier<String> msgFactory) {
        String message = msgFactory != ProgressLogger.NO_MESSAGE ? msgFactory.get() : null;
        int nextPercentage = (int) ((progressCounter.sum() / (double) taskVolume) * 100);
        if (globalPercentage < nextPercentage) {
            globalPercentage = nextPercentage;
            if (message == null || message.isEmpty()) {
                log.info("[%s] %s %d%%", Thread.currentThread().getName(), task, nextPercentage);
            } else {
                log.info(
                    "[%s] %s %d%% %s",
                    Thread.currentThread().getName(),
                    task,
                    nextPercentage,
                    message
                );
            }
        }
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        log.info("[%s] %s %s", Thread.currentThread().getName(), task, msg.get());
    }

    @Override
    public void reset(long newTaskVolume) {
        this.taskVolume = newTaskVolume;
        this.batchSize = calculateBatchSize(newTaskVolume, concurrency);
        progressCounter.reset();
        globalPercentage = -1;
    }

    @Override
    public Log getLog() {
        return this.log;
    }

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        throw new UnsupportedOperationException("BatchProgressLogger does not support logging percentages");
    }
}
