/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

public abstract class ExporterBuilder<T> {
    public static final String TASK_EXPORT = "EXPORT";

    final GraphDatabaseAPI db;
    final LongUnaryOperator toOriginalId;
    final long nodeCount;

    TerminationFlag terminationFlag;
    ExecutorService executorService;
    ProgressLoggerAdapter loggerAdapter;
    int writeConcurrency;

    ExporterBuilder(GraphDatabaseAPI db, IdMapping idMapping) {
        Objects.requireNonNull(idMapping);
        this.db = Objects.requireNonNull(db);
        this.nodeCount = idMapping.nodeCount();
        this.toOriginalId = idMapping::toOriginalNodeId;
        this.writeConcurrency = Pools.DEFAULT_CONCURRENCY;
    }

    public abstract T build();

    public ExporterBuilder<T> withLog(Log log) {
        loggerAdapter = new ProgressLoggerAdapter(Objects.requireNonNull(log), TASK_EXPORT);
        return this;
    }

    public ExporterBuilder<T> parallel(ExecutorService es, int writeConcurrency, TerminationFlag flag) {
        this.executorService = es;
        this.writeConcurrency = Pools.allowedConcurrency(writeConcurrency);
        this.terminationFlag = flag;
        return this;
    }
}
