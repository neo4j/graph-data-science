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
package org.neo4j.gds.similarity.filteredknn;

import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface FilteredKnnBaseConfig extends KnnBaseConfig {

    @Value.Default
    @Configuration.LongRange(min = 0)
    default List<Long> sourceNodeFilter() {
        return List.of();
    }

    @Value.Default
    @Configuration.LongRange(min = 0)
    default List<Long> targetNodeFilter() {
        return List.of();
    }

    @Configuration.GraphStoreValidationCheck
    default void validateSourceNodeFilter(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var nodes = graphStore.nodes();
        var missingNodes = sourceNodeFilter()
            .stream()
            .filter(n -> nodes.toMappedNodeId(n) == IdMap.NOT_FOUND)
            .collect(Collectors.toList());
        if (!missingNodes.isEmpty()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Invalid configuration value 'sourceNodeFilter', the following nodes are missing from the graph: %s",
                    missingNodes
                )
            );
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateTargetNodeFilter(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var nodes = graphStore.nodes();
        var missingNodes = targetNodeFilter()
            .stream()
            .filter(n -> nodes.toMappedNodeId(n) == IdMap.NOT_FOUND)
            .collect(Collectors.toList());
        if (!missingNodes.isEmpty()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Invalid configuration value 'targetNodeFilter', the following nodes are missing from the graph: %s",
                    missingNodes
                )
            );
        }
    }
}
