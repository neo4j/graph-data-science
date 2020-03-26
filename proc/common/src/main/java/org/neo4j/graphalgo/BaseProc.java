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

import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphLoader;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Collection;
import java.util.function.Supplier;

public abstract class BaseProc {

    protected static final String ESTIMATE_DESCRIPTION = "Returns an estimation of the memory consumption for that procedure.";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Context
    public ProcedureCallContext callContext;

    protected String getUsername() {
        return transaction.subjectOrAnonymous().username();
    }

    protected final GraphLoader newLoader(GraphCreateConfig createConfig, AllocationTracker tracker) {
        return ImmutableGraphLoader
            .builder()
            .api(api)
            .log(log)
            .username(getUsername())
            .tracker(tracker)
            .terminationFlag(TerminationFlag.wrap(transaction))
            .createConfig(createConfig)
            .build();
    }

    protected final void runWithExceptionLogging(String message, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.warn(message, e);
            throw e;
        }
    }

    protected final <R> R runWithExceptionLogging(String message, Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            log.warn(message, e);
            throw e;
        }
    }

    protected final void validateConfig(CypherMapWrapper cypherConfig, BaseConfig config) {
        validateConfig(cypherConfig, config.configKeys());
    }

    final void validateConfig(CypherMapWrapper cypherConfig, Collection<String> allowedKeys) {
        cypherConfig.requireOnlyKeysFrom(allowedKeys);
    }

    protected final void validateGraphName(String username, String graphName) {
        CypherMapWrapper.failOnBlank("graphName", graphName);
        if (GraphStoreCatalog.exists(username, graphName)) {
            throw new IllegalArgumentException(String.format(
                "A graph with name '%s' already exists.",
                graphName
            ));
        }
    }

    protected void validateMemoryUsage(MemoryTreeWithDimensions memoryTreeWithDimensions) {
        validateMemoryUsage(memoryTreeWithDimensions, Runtime.getRuntime()::freeMemory);
    }

    public void validateMemoryUsage(
        MemoryTreeWithDimensions memoryTreeWithDimensions,
        AlgoBaseProc.FreeMemoryInspector inspector
    ) {
        long freeMemory = inspector.freeMemory();
        long minBytesProcedure = memoryTreeWithDimensions.memoryTree.memoryUsage().min;
        if (minBytesProcedure > freeMemory) {
            throw new IllegalStateException(String.format(
                "Procedure was blocked since minimum estimated memory (%s) exceeds current free memory (%s).",
                MemoryUsage.humanReadable(minBytesProcedure),
                MemoryUsage.humanReadable(freeMemory)
            ));
        }
    }

    @FunctionalInterface
    public interface FreeMemoryInspector {
        long freeMemory();
    }
}
