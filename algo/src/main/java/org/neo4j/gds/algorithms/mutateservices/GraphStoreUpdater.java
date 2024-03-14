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
package org.neo4j.gds.algorithms.mutateservices;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.huge.FilteredNodePropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Extracting some common code so that it is reusable; eventually this can probably move to where it is used
 */
public final class GraphStoreUpdater {
    private GraphStoreUpdater() {}

    public static AddNodePropertyResult addNodeProperty(
        Graph graph,
        GraphStore graphStore,
        Collection<NodeLabel> labelsToUpdate,
        String propertyKey,
        NodePropertyValues propertyValues,
        Log log
    ) {
        var maybeFilteredNodePropertyValues = graph
            .asNodeFilteredGraph()
            .map(filteredGraph ->
                FilteredNodePropertyValues.OriginalToFilteredNodePropertyValues.create(
                        propertyValues,
                        filteredGraph
                    ))
            .orElse(propertyValues);

        log.info("Updating in-memory graph store");

        var mutateMilliseconds = new AtomicLong();
        try (ProgressTimer ignored = ProgressTimer.start(mutateMilliseconds::set)) {
            graphStore.addNodeProperty(
                new HashSet<>(labelsToUpdate),
                propertyKey,
                maybeFilteredNodePropertyValues
            );
        }

        return new AddNodePropertyResult(graph.nodeCount(), mutateMilliseconds.get());
    }

    public static AddRelationshipResult addRelationship(
        GraphStore graphStore,
        String mutateRelationshipType,
        String mutateProperty,
        SingleTypeRelationshipsProducer singleTypeRelationshipsProducer,
        Log log
    ) {
        var mutateMilliseconds = new AtomicLong();
        try (ProgressTimer ignored = ProgressTimer.start(mutateMilliseconds::set)) {

            var resultRelationships = singleTypeRelationshipsProducer.createRelationships(
                mutateRelationshipType,
                mutateProperty
            );

            log.info("Updating in-memory graph store");

            graphStore.addRelationshipType(resultRelationships);
        }
        return new AddRelationshipResult(
            singleTypeRelationshipsProducer.relationshipsCount(),
            mutateMilliseconds.get()
        );
    }

}
