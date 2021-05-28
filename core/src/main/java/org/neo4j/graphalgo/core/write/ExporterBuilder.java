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
package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

public abstract class ExporterBuilder<T> {
    final TransactionContext tx;
    final LongUnaryOperator toOriginalId;
    final long nodeCount;
    final TerminationFlag terminationFlag;

    ExecutorService executorService;
    ProgressLogger progressLogger;
    ProgressEventTracker eventTracker;
    int writeConcurrency;

    ExporterBuilder(TransactionContext tx, IdMapping idMapping, TerminationFlag terminationFlag) {
        Objects.requireNonNull(idMapping);
        this.tx = Objects.requireNonNull(tx);
        this.nodeCount = idMapping.nodeCount();
        this.toOriginalId = idMapping::toOriginalNodeId;
        this.writeConcurrency = ConcurrencyConfig.DEFAULT_CONCURRENCY;
        this.terminationFlag = terminationFlag;
        this.progressLogger = ProgressLogger.NULL_LOGGER;
        this.eventTracker = EmptyProgressEventTracker.INSTANCE;
    }

    public abstract T build();

    abstract String taskName();

    abstract long taskVolume();

    public ExporterBuilder<T> withLog(Log log) {
        return withLog(log, eventTracker);
    }

    public ExporterBuilder<T> withLog(Log log, ProgressEventTracker eventTracker) {
        return withProgressLogger(new BatchingProgressLogger(log, taskVolume(), taskName(), writeConcurrency, eventTracker));
    }

    public ExporterBuilder<T> withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    public ExporterBuilder<T> parallel(ExecutorService es, int writeConcurrency) {
        this.executorService = es;
        this.writeConcurrency = writeConcurrency;
        return this;
    }
}
