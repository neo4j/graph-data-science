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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestProgressLogger extends TestLog implements ProgressLogger {
    private final BatchingProgressLogger batchingLogger;
    private final List<AtomicLong> progresses;

    public TestProgressLogger(long initialTaskVolume, String task) {
        super();
        this.batchingLogger = new BatchingProgressLogger(this, initialTaskVolume, task);
        progresses = new ArrayList<>();
        progresses.add(new AtomicLong(0));
    }

    public List<AtomicLong> getProgresses() {
        return progresses;
    }

    @Override
    public void logProgress(Supplier<String> msgFactory) {
        progresses.get(progresses.size() - 1).incrementAndGet();
        batchingLogger.logProgress(msgFactory);
    }

    @Override
    public void logProgress(long progress, Supplier<String> msgFactory) {
        progresses.get(progresses.size() - 1).addAndGet(progress);
        batchingLogger.logProgress(progress, msgFactory);
    }

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        throw new UnsupportedOperationException("Deprecated");
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        batchingLogger.logMessage(msg);
    }

    @Override
    public void reset(long newTaskVolume) {
        progresses.add(new AtomicLong(0));
        batchingLogger.reset(newTaskVolume);
    }

    @Override
    public Log getLog() {
        return this;
    }
}


