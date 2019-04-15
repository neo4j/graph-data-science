/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.neo4j.logging.Log;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public class ProgressLoggerAdapter implements ProgressLogger {

    private final Log log;

    private final String task;

    private int logIntervalMillis = 10_000; // 10s log interval by default

    private AtomicLong lastLog = new AtomicLong(0L);

    public ProgressLoggerAdapter(Log log, String task) {
        this.log = log;
        this.task = task;
    }

    @Override
    public void logProgress(double percentDone, Supplier<String> msgFactory) {
        final long currentTime = System.currentTimeMillis();
        final long lastLogTime = lastLog.get();
        if (currentTime > lastLogTime + logIntervalMillis && lastLog.compareAndSet(lastLogTime, currentTime)) {
            doLog((int) (percentDone * 100), msgFactory);
        }
    }

    @Override
    public void log(Supplier<String> msgFactory) {
        doLog(100, msgFactory);
    }

    public void withLogIntervalMillis(int logIntervalMillis) {
        this.logIntervalMillis = logIntervalMillis;
    }

    private void doLog(int percent, Supplier<String> msgFactory) {
        String message = msgFactory != ProgressLogger.NO_MESSAGE ? msgFactory.get() : null;
        if (message == null || message.isEmpty()) {
            log.info("[%s] %s %d%%", Thread.currentThread().getName(), task, percent);
        } else {
            log.info("[%s] %s %d%% %s", Thread.currentThread().getName(), task, percent, message);
        }
    }
}
