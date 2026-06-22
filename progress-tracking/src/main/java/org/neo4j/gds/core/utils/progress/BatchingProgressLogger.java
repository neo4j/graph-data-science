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
package org.neo4j.gds.core.utils.progress;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class BatchingProgressLogger implements ProgressLogger {
    public static final long MAXIMUM_LOG_INTERVAL = (long) Math.pow(2, 13);

    private static final BatchSizeCalculator BatchSizeCalculator = new BatchSizeCalculator();

    private final CloseableThreadLocal<MutableLong> callCounter = CloseableThreadLocal.withInitial(MutableLong::new);
    private final LongAdder progressCounter = new LongAdder();

    private final LoggerForProgressTracking loggerForProgressTracking;
    private final RequestCorrelationId requestCorrelationId;
    private final Concurrency concurrency;

    private int globalPercentage = -1;

    private long taskVolume;
    private long batchSize;
    private String taskName;

    /**
     * A little bit of convenience
     */
    public static BatchingProgressLogger create(
        LoggerForProgressTracking log,
        RequestCorrelationId requestCorrelationId,
        Task task,
        Concurrency concurrency
    ) {
        var taskVolume = task.getProgress().volume();
        var batchSize = BatchSizeCalculator.calculateBatchSize(Math.max(1L, taskVolume), concurrency);
        var description = task.description();

        return new BatchingProgressLogger(log, requestCorrelationId, taskVolume, batchSize, description, concurrency);
    }

    /**
     * This is the only constructor, and it is just assignments
     */
    BatchingProgressLogger(
        LoggerForProgressTracking loggerForProgressTracking,
        RequestCorrelationId requestCorrelationId,
        long taskVolume,
        long batchSize,
        String taskName,
        Concurrency concurrency
    ) {
        this.loggerForProgressTracking = loggerForProgressTracking;
        this.requestCorrelationId = requestCorrelationId;
        this.taskVolume = taskVolume;
        this.batchSize = batchSize;
        this.taskName = taskName;
        this.concurrency = concurrency;
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
        callCounter.close();
    }

    private synchronized void doLogPercentage(Supplier<String> msgFactory, long progress) {
        var message = msgFactory.get();
        progressCounter.add(progress);
        int nextPercentage = (int) ((progressCounter.sum() / (double) taskVolume) * 100);
        if (globalPercentage < nextPercentage && globalPercentage < 100) {
            globalPercentage = nextPercentage;
            if (message == null || message.isEmpty()) {
                logProgress(nextPercentage);
            } else {
                logProgressWithMessage(nextPercentage, message);
            }
        }
    }

    private void logProgress(int nextPercentage) {
        logMessage(formatWithLocale("%d%%", nextPercentage));
    }

    private void logProgressWithMessage(int nextPercentage, String msg) {
        logMessage(formatWithLocale("%d%% %s", nextPercentage, msg));
    }

    @Override
    public void logMessage(String msg) {
        loggerForProgressTracking.info("[%s] [%s] %s %s", requestCorrelationId.toString(), Thread.currentThread().getName(), taskName, msg);
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        logMessage(Objects.requireNonNull(msg.get()));
    }

    @Override
    public long reset(long newTaskVolume) {
        var remainingVolume = taskVolume - progressCounter.sum();
        this.taskVolume = newTaskVolume;
        this.batchSize = BatchSizeCalculator.calculateBatchSize(newTaskVolume, concurrency);
        progressCounter.reset();
        globalPercentage = -1;
        return remainingVolume;
    }
}
