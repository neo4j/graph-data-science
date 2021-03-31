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
package org.neo4j.graphalgo.core.utils.export.file;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.loading.construction.ImmutablePropertyConfig;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilderBuilder;

import java.util.Map;
import java.util.stream.Collectors;

public class GraphStoreRelationshipVisitor extends RelationshipVisitor {

    private final Map<String, RelationshipsBuilder> relationshipBuilders;
    private final RelationshipsBuilderBuilder relationshipsBuilderBuilder;

    protected GraphStoreRelationshipVisitor(
        RelationshipSchema relationshipSchema,
        RelationshipsBuilderBuilder relationshipsBuilderBuilder,
        Map<String, RelationshipsBuilder> relationshipBuilders
    ) {
        super(relationshipSchema);
        this.relationshipsBuilderBuilder = relationshipsBuilderBuilder;
        this.relationshipBuilders = relationshipBuilders;
    }

    @Override
    protected void exportElement() {
        // TODO: this logic should move to the RelationshipsBuilder
        if (elementSchema.hasProperties(RelationshipType.of(relationshipType()))) {
            var relationshipsBuilder = relationshipBuilders.computeIfAbsent(
                relationshipType(),
                (key) -> relationshipsBuilderBuilder
                    .propertyConfigs(
                        getPropertySchema().stream().map(schema ->
                            ImmutablePropertyConfig.of(schema.aggregation(), schema.defaultValue())
                        ).collect(Collectors.toList())
                    )
                    .build()
            );

            var numberOfProperties = getPropertySchema().size();
            if(numberOfProperties > 1) {
                double[] propertyValues = new double[numberOfProperties];
                MutableInt index = new MutableInt();
                forEachProperty((propertyKey, propertyValue, valueType) -> {
                    propertyValues[index.getAndIncrement()] = Double.parseDouble(propertyValue.toString());
                });
                relationshipsBuilder.add(startNode(), endNode(), propertyValues);
            } else {
                forEachProperty((propertyKey, propertyValue, valueType) -> {
                    relationshipsBuilder.add(startNode(), endNode(), Double.parseDouble(propertyValue.toString()));
                });
            }
        } else {
            var relationshipsBuilder = relationshipBuilders.computeIfAbsent(
                relationshipType(),
                (key) -> relationshipsBuilderBuilder
                    .build()
            );
            relationshipsBuilder.add(startNode(), endNode());
        }
    }

    static final class Builder extends RelationshipVisitor.Builder<Builder, GraphStoreRelationshipVisitor> {

        RelationshipsBuilderBuilder relationshipsBuilderBuilder;
        Map<String, RelationshipsBuilder> relationshipBuildersByType;

        Builder withRelationshipBuilderBuilder(RelationshipsBuilderBuilder relationshipBuilderBuilder) {
            this.relationshipsBuilderBuilder = relationshipBuilderBuilder;
            return this;
        }

        Builder withRelationshipBuildersToTypeResultMap(Map<String, RelationshipsBuilder> relationshipBuildersByType) {
            this.relationshipBuildersByType = relationshipBuildersByType;
            return this;
        }

        @Override
        Builder me() {
            return this;
        }

        @Override
        GraphStoreRelationshipVisitor build() {
            return new GraphStoreRelationshipVisitor(
                relationshipSchema,
                relationshipsBuilderBuilder,
                relationshipBuildersByType
            );
        }
    }

    @ValueClass
    interface RelationshipVisitorResult {
        Map<RelationshipType, Relationships.Topology> relationshipTypesWithTopology();
        Map<? extends RelationshipType,? extends RelationshipPropertyStore> propertyStores();
        long relationshipCount();
    }
}
