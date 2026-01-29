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
package org.neo4j.gds.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;

import java.util.Collection;

public class CliqueCountingMutateStep implements MutateStep<CliqueCountingResult, NodePropertiesWritten> {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final String mutateProperty;
    private final Collection<String> nodeLabels;

    public CliqueCountingMutateStep(
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty
    ) {
        this.mutateProperty = mutateProperty;
        this.nodeLabels = labelsToUpdate;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    @Override
    public NodePropertiesWritten execute(
        Graph graph,
        GraphStore graphStore,
        CliqueCountingResult result
    ) {
        var nodePropertyValues = NodePropertyValuesAdapter.adapt(result.perNodeCount());

        return mutateNodePropertyService.mutateNodeProperties(
            graph,
            graphStore,
            mutateProperty,
            nodeLabels,
            nodePropertyValues
        );
    }
}
