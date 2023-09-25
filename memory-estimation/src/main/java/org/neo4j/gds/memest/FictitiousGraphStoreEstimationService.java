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
package org.neo4j.gds.memest;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.projection.GraphProjectFromStoreConfig;

import java.util.Collections;
import java.util.Set;

import static java.util.function.Predicate.isEqual;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;

public class FictitiousGraphStoreEstimationService {
    public GraphMemoryEstimation estimate(GraphProjectConfig graphProjectConfig) {
        var dimensions = graphDimensions(graphProjectConfig);
        var estimateMemoryUsageAfterLoading = estimateMemoryUsageAfterLoading(graphProjectConfig, dimensions);
        return new GraphMemoryEstimation(dimensions, estimateMemoryUsageAfterLoading);
    }

    private GraphDimensions graphDimensions(GraphProjectConfig graphProjectConfig) {
        var labelCount = 0;
        if (graphProjectConfig instanceof GraphProjectFromStoreConfig) {
            var storeConfig = (GraphProjectFromStoreConfig) graphProjectConfig;
            Set<NodeLabel> nodeLabels = storeConfig.nodeProjections().projections().keySet();
            labelCount = nodeLabels.stream().allMatch(isEqual(NodeLabel.ALL_NODES)) ? 0 : nodeLabels.size();
        }

        return ImmutableGraphDimensions.builder()
            .nodeCount(graphProjectConfig.nodeCount())
            .highestPossibleNodeCount(graphProjectConfig.nodeCount())
            .estimationNodeLabelCount(labelCount)
            .relationshipCounts(Collections.singletonMap(ALL_RELATIONSHIPS, graphProjectConfig.relationshipCount()))
            .relCountUpperBound(Math.max(graphProjectConfig.relationshipCount(), 0))
            .build();
    }

    private MemoryEstimation estimateMemoryUsageAfterLoading(GraphProjectConfig graphProjectConfig, GraphDimensions graphDimensions) {
        return graphProjectConfig
            .graphStoreFactory()
            .getWithDimension(GraphLoaderContext.NULL_CONTEXT, graphDimensions)
            .estimateMemoryUsageAfterLoading();
    }
}
