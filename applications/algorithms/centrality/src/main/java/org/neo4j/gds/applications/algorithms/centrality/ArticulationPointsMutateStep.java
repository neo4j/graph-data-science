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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;

class ArticulationPointsMutateStep implements MutateStep<BitSet, NodePropertiesWritten> {
    private final MutateNodeProperty mutateNodeProperty;
    private final ArticulationPointsMutateConfig configuration;

    ArticulationPointsMutateStep(MutateNodeProperty mutateNodeProperty, ArticulationPointsMutateConfig configuration) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        BitSet result
    ) {
        var nodeProperties =  new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return result.get(nodeId) ? 1 : 0;
            }

            @Override
            public long nodeCount() {
                return graph.nodeCount();
            }
        };

        return mutateNodeProperty.mutateNodeProperties(
            graph,
            graphStore,
            configuration,
            nodeProperties
        );
    }
}
