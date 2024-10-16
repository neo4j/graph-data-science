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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.HashSet;

public class MutateNodeProperty {
    private final Log log;

    public MutateNodeProperty(Log log) {
        this.log = log;
    }

    public NodePropertiesWritten mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        MutateNodePropertyConfig configuration,
        NodePropertyValues nodePropertyValues)
    {
       return mutateNodeProperties(
           graph,
           graphStore,
           configuration.nodeLabelIdentifiers(graphStore),
           configuration.mutateProperty(),
           nodePropertyValues
       );
    }

    public NodePropertiesWritten mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        Collection<NodeLabel> labelsToUpdate,
        String mutateProperty,
        NodePropertyValues nodePropertyValues
    ) {
        var maybeFilteredNodePropertyValues = graph
            .asNodeFilteredGraph()
            .map(filteredGraph ->
                FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(
                    nodePropertyValues,
                    filteredGraph
                ))
            .orElse(nodePropertyValues);

        log.info("Updating in-memory graph store");

        graphStore.addNodeProperty(
            new HashSet<>(labelsToUpdate),
            mutateProperty,
            maybeFilteredNodePropertyValues
        );

        return new NodePropertiesWritten(graph.nodeCount());
    }
}
