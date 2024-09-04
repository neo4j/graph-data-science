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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;

class FastRPMutateStep implements MutateStep<FastRPResult, NodePropertiesWritten> {
    private final MutateNodeProperty mutateNodeProperty;
    private final FastRPMutateConfig configuration;

    FastRPMutateStep(MutateNodeProperty mutateNodeProperty, FastRPMutateConfig configuration) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        FastRPResult result
    ) {
        var nodePropertyValues = NodePropertyValuesAdapter.adapt(result.embeddings());

        return mutateNodeProperty.mutateNodeProperties(
            graph,
            graphStore,
            configuration,
            nodePropertyValues
        );
    }
}
