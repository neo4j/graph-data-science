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
package org.neo4j.gds.core.io;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.io.file.RelationshipBuilderFromVisitor;
import org.neo4j.gds.core.io.file.RelationshipVisitor;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilderBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GraphStoreRelationshipVisitor extends RelationshipVisitor {

    private final Supplier<RelationshipsBuilderBuilder> relationshipBuilderSupplier;
    private final Map<String, RelationshipsBuilder> relationshipBuilders;
    private final List<RelationshipType> inverseIndexedRelationshipTypes;
    private final Map<String, RelationshipBuilderFromVisitor> relationshipFromVisitorBuilders;

    public GraphStoreRelationshipVisitor(
        RelationshipSchema relationshipSchema,
        Supplier<RelationshipsBuilderBuilder> relationshipBuilderSupplier,
        Map<String, RelationshipsBuilder> relationshipBuilders,
        List<RelationshipType> inverseIndexedRelationshipTypes
    ) {
        super(relationshipSchema);
        this.relationshipBuilderSupplier = relationshipBuilderSupplier;
        this.relationshipBuilders = relationshipBuilders;
        this.inverseIndexedRelationshipTypes = inverseIndexedRelationshipTypes;
        relationshipFromVisitorBuilders = new HashMap<>();
    }

    @Override
    protected void exportElement() {
        // TODO: this logic should move to the RelationshipsBuilder
        var relationshipsBuilder = relationshipFromVisitorBuilders.computeIfAbsent(
                relationshipType(),
                (relationshipTypeString) -> {
                    var propertyConfigs = getPropertySchema()
                        .stream()
                        .map(schema -> GraphFactory.PropertyConfig.of(schema.key(), schema.aggregation(), schema.defaultValue()))
                        .collect(Collectors.toList());
                    RelationshipType relationshipType = RelationshipType.of(relationshipTypeString);

                    var relBuilder = relationshipBuilderSupplier.get()
                        .relationshipType(relationshipType)
                        .propertyConfigs(propertyConfigs)
                        .indexInverse(inverseIndexedRelationshipTypes.contains(relationshipType))
                        .build();
                    relationshipBuilders.put(relationshipTypeString, relBuilder);
                    return RelationshipBuilderFromVisitor.of(
                        propertyConfigs.size(),
                        relBuilder,
                        GraphStoreRelationshipVisitor.this
                    );
                }
            );
        relationshipsBuilder.addFromVisitor();
    }

    public static final class Builder extends RelationshipVisitor.Builder<Builder, GraphStoreRelationshipVisitor> {

        Map<String, RelationshipsBuilder> relationshipBuildersByType;
        int concurrency;
        IdMap nodes;
        List<RelationshipType> inverseIndexedRelationshipTypes;

        public Builder withRelationshipBuildersToTypeResultMap(Map<String, RelationshipsBuilder> relationshipBuildersByType) {
            this.relationshipBuildersByType = relationshipBuildersByType;
            return this;
        }

        public Builder withConcurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder withNodes(IdMap nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withAllocationTracker() {
            return this;
        }


        public Builder withInverseIndexedRelationshipTypes(List<RelationshipType> inverseIndexedRelationshipTypes) {
            this.inverseIndexedRelationshipTypes = inverseIndexedRelationshipTypes;
            return this;
        }

        @Override
        public Builder me() {
            return this;
        }

        @Override
        public GraphStoreRelationshipVisitor build() {
            Supplier<RelationshipsBuilderBuilder> relationshipBuilderSupplier = () -> GraphFactory
                .initRelationshipsBuilder()
                .concurrency(concurrency)
                .nodes(nodes);
            return new GraphStoreRelationshipVisitor(
                relationshipSchema,
                relationshipBuilderSupplier,
                relationshipBuildersByType,
                inverseIndexedRelationshipTypes
            );
        }
    }

    @ValueClass
    interface RelationshipVisitorResult {
        Map<RelationshipType, Topology> relationshipTypesWithTopology();
        Map<RelationshipType, RelationshipPropertyStore> propertyStores();
        long relationshipCount();
    }
}
