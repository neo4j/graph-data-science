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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.GraphStoreService;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;

/**
 * Extracting some common code so that it is reusable; eventually this can probably move to where it is used
 */
public final class GraphStoreUpdater {
    private GraphStoreUpdater() {}

    @Deprecated(forRemoval = true)
    public static <ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutateNodePropertyConfig> void UpdateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext,
        final List<NodeProperty> nodePropertyList
    ) {
        updateGraphStore(
            computationResult.graph(),
            computationResult.graphStore(),
            resultBuilder,
            computationResult.config(),
            executionContext.log(),
            nodePropertyList
        );
    }

    /**
     * Let's eventually get rid of this
     */
    @Deprecated
    public static void updateGraphStore(
        Graph graph,
        GraphStore graphStore,
        AbstractResultBuilder<?> resultBuilder,
        AlgoBaseConfig configuration,
        Log log,
        final List<NodeProperty> nodePropertyList
    ) {
        var nodePropertiesWritten = new GraphStoreService(log).addNodeProperties(
            graph,
            graphStore,
            configuration,
            nodePropertyList
        );

        resultBuilder.withNodePropertiesWritten(nodePropertiesWritten.value());
    }
}
