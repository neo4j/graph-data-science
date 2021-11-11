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

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static java.util.function.Predicate.isEqual;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class FictitiousGraphStoreLoader implements GraphStoreLoader {

    private final GraphCreateConfig graphCreateConfig;

    FictitiousGraphStoreLoader(GraphCreateConfig graphCreateConfig) {
        this.graphCreateConfig = graphCreateConfig;
    }

    @Override
    public GraphCreateConfig graphCreateConfig() {
        return this.graphCreateConfig;
    }

    @Override
    public GraphDimensions graphDimensions() {
        var labelCount = 0;
        if (graphCreateConfig() instanceof GraphCreateFromStoreConfig) {
            var storeConfig = (GraphCreateFromStoreConfig) graphCreateConfig();
            Set<NodeLabel> nodeLabels = storeConfig.nodeProjections().projections().keySet();
            labelCount = nodeLabels.stream().allMatch(isEqual(NodeLabel.ALL_NODES)) ? 0 : nodeLabels.size();
        }

        return ImmutableGraphDimensions.builder()
            .nodeCount(graphCreateConfig().nodeCount())
            .highestPossibleNodeCount(graphCreateConfig().nodeCount())
            .estimationNodeLabelCount(labelCount)
            .relationshipCounts(Collections.singletonMap(ALL_RELATIONSHIPS, graphCreateConfig().relationshipCount()))
            .maxRelCount(Math.max(graphCreateConfig().relationshipCount(), 0))
            .build();
    }

    @Override
    public Optional<MemoryEstimation> memoryEstimation() {
        return Optional.of(graphCreateConfig
            .graphStoreFactory()
            .getWithDimension(GraphLoaderContext.NULL_CONTEXT, graphDimensions())
            .memoryEstimation()
        );
    }

    @Override
    public GraphStore graphStore() {
        throw new UnsupportedOperationException(formatWithLocale(
            "%s does not support creating a graph store",
            this.getClass().getSimpleName()
        ));
    }
}
