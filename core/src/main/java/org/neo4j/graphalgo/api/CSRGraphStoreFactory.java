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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.values.storable.NumberType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
        Map<RelationshipType, RelationshipPropertyStore> relationshipPropertyStores = new HashMap<>(relTypeCount);

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
                propertyMappings.enumerate().forEach(propertyIndexAnMapping -> {
                    int propertyIndex = propertyIndexAnMapping.getOne();
                    PropertyMapping propertyMapping = propertyIndexAnMapping.getTwo();
                    RelationshipPropertyStore.Builder propertyStoreBuilder = RelationshipPropertyStore.builder();
                    propertyStoreBuilder.putIfAbsent(
                        propertyMapping.propertyKey(),
                        RelationshipProperty.of(
                            propertyMapping.propertyKey(),
                            NumberType.FLOATING_POINT,
                            GraphStore.PropertyState.PERSISTENT,
                            ImmutableProperties.of(
                                relationshipsBuilder.properties(),
                                relationshipsBuilder.globalPropertyOffsets(propertyIndex),
                                relationshipCount,
                                projection.orientation(),
                                projection.isMultiGraph(),
                                propertyMapping.defaultValue().doubleValue() // This is fine because relationships currently only support doubles
                            ),
                            propertyMapping.defaultValue(),
                            propertyMapping.aggregation()
                        )
                    );
                    relationshipPropertyStores.put(relationshipType, propertyStoreBuilder.build());
                });
            }
        });

        return new CSRGraphStore(
            loadingContext.api().databaseId(),
            idsAndProperties.idMap(),
            idsAndProperties.properties(),
            relationships,
            relationshipPropertyStores,
            graphCreateConfig.readConcurrency(),
            tracker
        );
    }

    protected void logLoadingSummary(GraphStore graphStore, Optional<AllocationTracker> tracker) {
        tracker.ifPresent(progressLogger::logMessage);

        var sizeInBytes = MemoryUsage.sizeOf(graphStore);
        var memoryUsage = MemoryUsage.humanReadable(sizeInBytes);
        progressLogger.logMessage(formatWithLocale("Actual memory usage of the loaded graph: %s", memoryUsage));
    }
}
