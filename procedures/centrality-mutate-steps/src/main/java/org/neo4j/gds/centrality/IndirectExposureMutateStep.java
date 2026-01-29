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
package org.neo4j.gds.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;

import java.util.Collection;
import java.util.List;

public class IndirectExposureMutateStep implements MutateStep<IndirectExposureResult, NodePropertiesWritten> {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> nodeLabels;
    private final String exposures;
    private final String hops;
    private final String parents;
    private final String roots;

    public IndirectExposureMutateStep(
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> nodeLabels,
        String exposures,
        String hops,
        String parents,
        String roots
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.exposures = exposures;
        this.hops = hops;
        this.parents = parents;
        this.roots = roots;
        this.nodeLabels = nodeLabels;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        IndirectExposureResult result
    ) {

        var exposure = NodePropertyRecord.of(exposures, result.exposureValues());
        var hops =  NodePropertyRecord.of(this.hops, result.hopValues());
        var parents = NodePropertyRecord.of(this.parents, result.parentValues());
        var roots = NodePropertyRecord.of(this.roots, result.rootValues());

        return mutateNodePropertyService.mutateNodeProperties(
            graph,
            graphStore,
            List.of(exposure, hops, parents, roots),
            nodeLabels
        );

    }

}
