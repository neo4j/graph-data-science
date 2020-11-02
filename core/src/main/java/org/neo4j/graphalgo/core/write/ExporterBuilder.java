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
package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

public abstract class ExporterBuilder<T> {
    final SecureTransaction tx;
    final LongUnaryOperator toOriginalId;
    final long nodeCount;
    final TerminationFlag terminationFlag;

    ExecutorService executorService;
    ProgressLogger progressLogger;
    int writeConcurrency;

    ExporterBuilder(SecureTransaction tx, IdMapping idMapping, TerminationFlag terminationFlag) {
        Objects.requireNonNull(idMapping);
        this.tx = Objects.requireNonNull(tx);
        this.nodeCount = idMapping.nodeCount();
        this.toOriginalId = idMapping::toOriginalNodeId;
        this.writeConcurrency = ConcurrencyConfig.DEFAULT_CONCURRENCY;
        this.terminationFlag = terminationFlag;
        this.progressLogger = ProgressLogger.NULL_LOGGER;
    }

    public abstract T build();

    public ExporterBuilder<T> withLog(Log log) {
        // Due to another bug, writeConcurrency is 0 by default which is handled in ParallelUtil
        // We mimic this here and will resolve this together with the bugfix
        var failSafeConcurrency = writeConcurrency == 0 ? 1 : writeConcurrency;
        progressLogger = new BatchingProgressLogger(log, nodeCount, "WriteNodeProperties", failSafeConcurrency);
        return this;
    }

    public ExporterBuilder<T> parallel(ExecutorService es, int writeConcurrency) {
        this.executorService = es;
        this.writeConcurrency = writeConcurrency;
        return this;
    }
}
