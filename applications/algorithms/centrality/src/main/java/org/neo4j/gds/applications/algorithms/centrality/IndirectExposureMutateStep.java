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
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;

class IndirectExposureMutateStep implements MutateStep<IndirectExposureResult, NodePropertiesWritten> {
    private final MutateNodeProperty mutateNodeProperty;
    private final IndirectExposureMutateConfig config;

    IndirectExposureMutateStep(
        MutateNodeProperty mutateNodeProperty,
        IndirectExposureMutateConfig configuration
    ) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.config = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        IndirectExposureResult result
    ) {
        var exposuresWritten = mutate(graph, graphStore, config.exposureProperty(), result.exposureValues());
        var hopsWritten = mutate(graph, graphStore, config.hopProperty(), result.hopValues());
        var parentsWritten = mutate(graph, graphStore, config.parentProperty(), result.parentValues());
        var rootsWritten = mutate(graph, graphStore, config.rootProperty(), result.rootValues());

        return new NodePropertiesWritten(
            exposuresWritten.value() + hopsWritten.value() + parentsWritten.value() + rootsWritten.value()
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
            config.nodeLabelIdentifiers(graphStore),
            mutateProperty,
            values
        );
    }
}
