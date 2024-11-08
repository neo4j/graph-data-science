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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.hits.HitsConfig;

class HitsMutateStep implements MutateStep<PregelResult, NodePropertiesWritten> {
    private final MutateNodeProperty mutateNodeProperty;
    private final HitsConfig configuration;

    HitsMutateStep(
        MutateNodeProperty mutateNodeProperty,
        HitsConfig configuration) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        PregelResult result
    ) {
        var authValues = NodePropertyValuesAdapter.adapt(result.nodeValues().doubleProperties(configuration.authProperty()));
        var hubValues = NodePropertyValuesAdapter.adapt(result.nodeValues().doubleProperties(configuration.authProperty()));
        var authProperty = configuration.authProperty().concat(configuration.mutateProperty());
        var hubProperty = configuration.hubProperty().concat(configuration.mutateProperty());

        var authWritten = mutate(graph, graphStore, authProperty,authValues);
        var hubWritten = mutate(graph, graphStore, hubProperty,hubValues);

        return new NodePropertiesWritten(
            authWritten.value() + hubWritten.value()
        );
    }

    private NodePropertiesWritten mutate(
        Graph graph,
        GraphStore graphStore,
        String mutateProperty,
        NodePropertyValues values
    ) {
        return mutateNodeProperty.mutateNodeProperties(
            graph,
            graphStore,
            configuration.nodeLabelIdentifiers(graphStore),
            mutateProperty,
            values
        );
    }
}
