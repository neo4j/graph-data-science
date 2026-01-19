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

import org.agrona.collections.MutableLong;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MutateNodePropertyService {
    private final Log log;

    public MutateNodePropertyService(Log log) {
        this.log = log;
    }

    public NodePropertiesWritten mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        MutateNodePropertySpec spec,
        NodePropertyValues nodePropertyValues
    )
    {
       return mutateNodeProperties(
           graph,
           graphStore,
           spec,
           List.of(NodePropertyRecord.of(spec.mutateProperty(), nodePropertyValues))
       );
    }

    public NodePropertiesWritten mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        MutateParameters parameters,
        List<NodePropertyRecord> nodeProperties
    )
    {
        return mutateNodeProperties(
            graph,
            graphStore,
            resolveNodeLabels(graphStore, parameters.nodeLabels()),
            nodeProperties
        );
    }

    private NodePropertiesWritten mutateNodeProperties(
        Graph graph,
        GraphStore graphStore,
        Collection<NodeLabel> labelsToUpdate,
        List<NodePropertyRecord> nodeProperties
    ) {
        log.info("Updating in-memory graph store");

        var translatedProperties = translateProperties(graph, nodeProperties);
        MutableLong nodePropertiesWritten = new MutableLong();
        translatedProperties.forEach(
            property ->{
                var written = mutateNodeProperty(graph,graphStore,property,labelsToUpdate);
                nodePropertiesWritten.addAndGet(written);
            }
        );

        return new NodePropertiesWritten(nodePropertiesWritten.longValue());
    }

    private long mutateNodeProperty(
        IdMap graph,
        GraphStore graphStore,
        NodePropertyRecord property,
        Collection<NodeLabel> labelsToUpdate
    ){
        graphStore.addNodeProperty(
            new HashSet<>(labelsToUpdate),
            property.key(),
            property.values()
        );
        return graph.nodeCount();
    }


    private List<NodePropertyRecord> translateProperties(Graph graph, List<NodePropertyRecord> nodeProperties) {
        return graph
            .asNodeFilteredGraph()
            .map(filteredGraph -> nodeProperties
                .stream()
                .map(nodeProperty -> NodePropertyRecord.of(
                    nodeProperty.key(),
                    FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(
                        nodeProperty.values(),
                        filteredGraph
                    )
                ))
                .collect(Collectors.toList()))
            .orElse(nodeProperties);
    }

    private Collection<NodeLabel> resolveNodeLabels(GraphStore graphStore,Collection<String> nodeLabels){
        return ElementTypeValidator.resolve(graphStore,nodeLabels);
    }

    public record MutateNodePropertySpec(String mutateProperty, Collection<String> nodeLabels) implements MutateParameters{}
    public record MutateNodePropertiesSpec(Collection<String> nodeLabels) implements MutateParameters{}
    public interface MutateParameters {
        Collection<String> nodeLabels();
    }

}
