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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.logging.Log;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class GraphStoreService {
    private final Log log;

    public GraphStoreService(Log log) {
        this.log = log;
    }

    public NodePropertiesWritten addNodeProperties(
        Graph graph,
        GraphStore graphStore,
        AlgoBaseConfig configuration,
        List<NodeProperty> nodeProperties
    ) {
        var translatedProperties = translateProperties(graph, nodeProperties);

        log.info("Updating in-memory graph store");
        var labelsToUpdate = configuration.nodeLabelIdentifiers(graphStore);

        translatedProperties.forEach(nodeProperty -> graphStore.addNodeProperty(
            new HashSet<>(labelsToUpdate),
            nodeProperty.key(),
            nodeProperty.values()
        ));

        return new NodePropertiesWritten(translatedProperties.size() * graph.nodeCount());
    }

    private List<NodeProperty> translateProperties(Graph graph, List<NodeProperty> nodeProperties) {
        return graph
            .asNodeFilteredGraph()
            .map(filteredGraph -> nodeProperties
                .stream()
                .map(nodeProperty -> NodeProperty.of(
                    nodeProperty.key(),
                    FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(
                        nodeProperty.values(),
                        filteredGraph
                    )
                ))
                .collect(Collectors.toList()))
            .orElse(nodeProperties);
    }
}
