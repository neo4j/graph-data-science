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
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService.MutateNodePropertiesSpec;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;

import java.util.List;

class IndirectExposureMutateStep implements MutateStep<IndirectExposureResult, NodePropertiesWritten> {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final IndirectExposureMutateConfig config;
    private final MutateNodePropertiesSpec mutateParameters;

    IndirectExposureMutateStep(
        MutateNodePropertyService mutateNodePropertyService,
        IndirectExposureMutateConfig configuration
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.config = configuration;
        this.mutateParameters = new MutateNodePropertiesSpec(configuration.nodeLabels());
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        IndirectExposureResult result
    ) {
        var mutateProperties = this.config.mutateProperties();

        var exposure = NodePropertyRecord.of(mutateProperties.exposures(), result.exposureValues());
        var hops =  NodePropertyRecord.of( mutateProperties.hops(), result.hopValues());
        var parents = NodePropertyRecord.of( mutateProperties.parents(), result.parentValues());
        var roots = NodePropertyRecord.of( mutateProperties.roots(), result.rootValues());

        return mutateNodePropertyService.mutateNodeProperties(
            graph,
            graphStore,
            mutateParameters,
            List.of(exposure, hops, parents, roots)
        );

    }

}
