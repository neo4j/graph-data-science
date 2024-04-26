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
package org.neo4j.gds.applications.algorithms.centrality;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.Map;
import java.util.function.LongToDoubleFunction;

public class MutateNodeProperty {
    private final MutateNodePropertyService mutateNodePropertyService;

    public MutateNodeProperty(MutateNodePropertyService mutateNodePropertyService) {
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    Pair<Map<String, Object>, NodePropertiesWritten> mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        MutateNodePropertyConfig configuration,
        LongToDoubleFunction centralityFunction,
        boolean shouldComputeCentralityDistribution,
        NodePropertyValues nodePropertyValues
    ) {
        // Compute result statistics
        var centralityStatistics = CentralityStatistics.centralityStatistics(
            graph.nodeCount(),
            centralityFunction,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            shouldComputeCentralityDistribution
        );

        var centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());

        // 3. Go and mutate the graph store
        var addNodePropertyResult = mutateNodePropertyService.mutate(
            configuration.mutateProperty(),
            nodePropertyValues,
            configuration.nodeLabelIdentifiers(graphStore),
            graph,
            graphStore
        );

        return Pair.of(centralitySummary, new NodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded()));
    }
}
