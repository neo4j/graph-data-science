/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class CSRGraphStoreFactory<CONFIG extends GraphCreateConfig> extends GraphStoreFactory<CSRGraphStore, CONFIG> {

    public CSRGraphStoreFactory(
        CONFIG graphCreateConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions
    ) {
        super(graphCreateConfig, loadingContext, dimensions);
    }

    protected CSRGraphStore createGraphStore(
        IdsAndProperties idsAndProperties,
        RelationshipImportResult relationshipImportResult,
        AllocationTracker tracker,
        GraphDimensions dimensions
    ) {
        int relTypeCount = dimensions.relationshipTypeTokens().size();
        Map<RelationshipType, Relationships.Topology> relationships = new HashMap<>(relTypeCount);
        Map<RelationshipType, Map<String, Relationships.Properties>> relationshipProperties = new HashMap<>(relTypeCount);

        relationshipImportResult.builders().forEach((relationshipType, relationshipsBuilder) -> {
            AdjacencyList adjacencyList = relationshipsBuilder.adjacencyList();
            AdjacencyOffsets adjacencyOffsets = relationshipsBuilder.globalAdjacencyOffsets();
            long relationshipCount = relationshipImportResult.counts().getOrDefault(relationshipType, 0L);

            RelationshipProjection projection = relationshipsBuilder.projection();

            relationships.put(
                relationshipType,
                ImmutableTopology.of(
                    adjacencyList,
                    adjacencyOffsets,
                    relationshipCount,
                    projection.orientation(),
                    projection.isMultiGraph()
                )
            );

            PropertyMappings propertyMappings = projection.properties();
            if (!propertyMappings.isEmpty()) {
                Map<String, Relationships.Properties> propertyMap = propertyMappings
                    .enumerate()
                    .collect(Collectors.toMap(
                        propertyIndexAndMapping -> propertyIndexAndMapping.getTwo().propertyKey(),
                        propertyIndexAndMapping -> ImmutableProperties.of(
                            relationshipsBuilder.properties(propertyIndexAndMapping.getOne()),
                            relationshipsBuilder.globalPropertyOffsets(propertyIndexAndMapping.getOne()),
                            relationshipCount,
                            projection.orientation(),
                            projection.isMultiGraph(),
                            propertyIndexAndMapping.getTwo().defaultValue().doubleValue() // This is fine because relationships currently only support doubles
                        )
                    ));
                relationshipProperties.put(relationshipType, propertyMap);
            }
        });

        return CSRGraphStore.of(
            loadingContext.api().databaseId(),
            idsAndProperties.idMap(),
            idsAndProperties.properties(),
            relationships,
            relationshipProperties,
            graphCreateConfig.readConcurrency(),
            tracker
        );
    }
}
