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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.RankConfig;

class IndirectExposureMutateStep implements MutateStep<HugeDoubleArray, NodePropertiesWritten> {
    private final MutateNodeProperty mutateNodeProperty;
    private final IndirectExposureMutateConfig configuration;

    IndirectExposureMutateStep(
        MutateNodeProperty mutateNodeProperty,
        IndirectExposureMutateConfig configuration
    ) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.configuration = configuration;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        HugeDoubleArray result
    ) {
        return mutateNodeProperty.mutateNodeProperties(
            graph,
            graphStore,
            configuration,
            NodePropertyValuesAdapter.adapt(result)
        );
    }
}
