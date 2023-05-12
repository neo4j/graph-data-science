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
package org.neo4j.gds.catalog;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Map;
import java.util.stream.Stream;

public final class GraphListOperator {

    public static final String NO_VALUE = "__NO_VALUE";

    public static Stream<GraphInfoWithHistogram> list(String graphName,ExecutionContext executionContext){
        var terminationFlag = TerminationFlag.wrap(executionContext.terminationMonitor());

        var graphEntries = executionContext.isGdsAdmin()
            ? GraphStoreCatalog.getAllGraphStores().map(graphStore -> Map.entry(graphStore.config(), graphStore.graphStore()))
            : GraphStoreCatalog.getGraphStores(executionContext.username()).entrySet().stream();

        if (graphName != null && !graphName.equals(NO_VALUE)) {
            validateGraphName(graphName);
            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphName));
        }

        return graphEntries.map(e -> {
            GraphProjectConfig graphProjectConfig = e.getKey();
            GraphStore graphStore = e.getValue();
            var returnColumns = executionContext.returnColumns();
            boolean computeDegreeDistribution = returnColumns.contains("degreeDistribution");

            boolean computeGraphSize = returnColumns.contains("memoryUsage") || returnColumns.contains("sizeInBytes");

            return GraphInfoWithHistogram.of(
                graphProjectConfig,
                graphStore,
                computeDegreeDistribution,
                computeGraphSize,
                terminationFlag
            );
        });

    }
    protected static @NotNull String validateGraphName(@Nullable String graphName) {
        return CypherMapAccess.failOnBlank("graphName", graphName);
    }
}
