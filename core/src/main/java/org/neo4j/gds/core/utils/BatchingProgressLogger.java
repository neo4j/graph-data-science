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
package org.neo4j.gds.core.utils;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class BatchingProgressLogger implements ProgressLogger {
    public static final long MAXIMUM_LOG_INTERVAL = (long) Math.pow(2, 13);

    private final Log log;
    private final int concurrency;
    private long taskVolume;
    private long batchSize;
    private String taskName;
    private final LongAdder progressCounter;
    private final ThreadLocal<MutableLong> callCounter;

    private int globalPercentage;

    private static long calculateBatchSize(Task task, int concurrency) {
        return calculateBatchSize(Math.max(1L, task.getProgress().volume()), concurrency);
    }

    private static long calculateBatchSize(long taskVolume, int concurrency) {
        // target 100 logs per full run (every 1 percent)
        var batchSize = taskVolume / 100;
        // split batchSize into thread-local chunks
        batchSize /= concurrency;
        // batchSize needs to be a power of two
        return Math.max(1, BitUtil.nextHighestPowerOfTwo(batchSize));
    }

    public BatchingProgressLogger(Log log, Task task, int concurrency) {
        this(log, task, calculateBatchSize(task, concurrency), concurrency);
    }

    public BatchingProgressLogger(Log log, Task task, long batchSize, int concurrency) {
        this.log = log;
        this.taskVolume = task.getProgress().volume();
        this.batchSize = batchSize;
        this.taskName = task.description();

        this.progressCounter = new LongAdder();
        this.callCounter = ThreadLocal.withInitial(MutableLong::new);
        this.concurrency = concurrency;
        this.globalPercentage = -1;
    }

    @Override
    public String getTask() {
        return taskName;
    }

    @Override
    public void setTask(String task) {
        this.taskName = task;
    }

    @Override
    public void logProgress(Supplier<String> msgFactory) {
        var localProgress = callCounter.get();
        if (localProgress.incrementAndGet() >= batchSize) {
            doLogPercentage(msgFactory, 1);
            localProgress.setValue(0L);
        } else {
            progressCounter.increment();
        }
    }

    @Override
    public void logProgress(long progress, Supplier<String> msgFactory) {
        if (progress == 0) {
            return;
        }
        var localProgress = callCounter.get();
        if (localProgress.addAndGet(progress) >= batchSize) {
            doLogPercentage(msgFactory, progress);
            localProgress.setValue(localProgress.longValue() & (batchSize - 1));
        } else {
            progressCounter.add(progress);
        }
    }

    @Override
    public void logFinishPercentage() {
        if (globalPercentage < 100) {
            logProgress(100);
        }
    }

    @Override
    public void release() {
    }

    private synchronized void doLogPercentage(Supplier<String> msgFactory, long progress) {
        String message = msgFactory != NO_MESSAGE ? msgFactory.get() : null;
        progressCounter.add(progress);
        int nextPercentage = (int) ((progressCounter.sum() / (double) taskVolume) * 100);
        if (globalPercentage < nextPercentage) {
            globalPercentage = nextPercentage;
            if (message == null || message.isEmpty()) {
                logProgress(nextPercentage);
            } else {
                logProgressWithMessage(nextPercentage, message);
            }
        }
    }

    private void logProgress(int nextPercentage) {
        logInfo(formatWithLocale("[%s] %s %d%%", Thread.currentThread().getName(), taskName, nextPercentage));
    }

    private void logProgressWithMessage(int nextPercentage, String msg) {
        logInfo(
            formatWithLocale("[%s] %s %d%% %s", Thread.currentThread().getName(), taskName, nextPercentage, msg)
        );
    }

    @Override
    public void logMessage(String msg) {
        log.info("[%s] %s %s", Thread.currentThread().getName(), taskName, msg);
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        logMessage(Objects.requireNonNull(msg.get()));
    }

    private void logInfo(String message) {
        log.info(message);
    }

    @Override
    public void logDebug(String message) {
        log.debug("[%s] %s %s", Thread.currentThread().getName(), taskName, message);
    }

    @Override
    public void logWarning(String message) {
        log.warn("[%s] %s %s", Thread.currentThread().getName(), taskName, message);
    }

    @Override
    public long reset(long newTaskVolume) {
        var remainingVolume = taskVolume - progressCounter.sum();
        this.taskVolume = newTaskVolume;
        this.batchSize = calculateBatchSize(newTaskVolume, concurrency);
        progressCounter.reset();
        globalPercentage = -1;
        return remainingVolume;
    }
}
