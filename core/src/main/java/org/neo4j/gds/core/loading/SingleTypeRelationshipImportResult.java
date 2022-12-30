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
package org.neo4j.gds.core.loading;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.ImmutableRelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;

import java.util.Optional;

@ValueClass
public interface SingleTypeRelationshipImportResult {

    SingleTypeRelationshipImportResult EMPTY = SingleTypeRelationshipImportResult
            .builder()
            .direction(Direction.DIRECTED)
            .topology(Relationships.Topology.EMPTY)
            .build();


    // TODO: remove
    Direction direction();

    // TODO: add RelationshipSchema

    Relationships.Topology topology();

    Optional<RelationshipPropertyStore> properties();

    Optional<Relationships.Topology> inverseTopology();

    Optional<RelationshipPropertyStore> inverseProperties();

    /**
     * Filters the relationships to include only the given property if present.
     */
    default SingleTypeRelationshipImportResult filter(String propertyKey) {
        var properties = properties().map(relationshipPropertyStore ->
            relationshipPropertyStore.filter(propertyKey)
        );
        var inverseProperties = inverseProperties().map(relationshipPropertyStore ->
            relationshipPropertyStore.filter(propertyKey)
        );

        return SingleTypeRelationshipImportResult.builder()
            .topology(topology())
            .direction(direction())
            .inverseTopology(inverseTopology())
            .properties(properties)
            .inverseProperties(inverseProperties)
            .build();
    }

    @Value.Check
    default SingleTypeRelationshipImportResult normalize() {
        if (properties().map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            return builder().from(this).properties(Optional.empty()).build();
        }
        if (inverseProperties().map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            return builder().from(this).inverseProperties(Optional.empty()).build();
        }
        return this;
    }

    static ImmutableSingleTypeRelationshipImportResult.Builder builder() {
        return ImmutableSingleTypeRelationshipImportResult.builder();
    }

    static SingleTypeRelationshipImportResult of(
        Relationships relationships,
        Direction direction,
        Optional<RelationshipPropertySchema> propertySchema
    ) {
        return SingleTypeRelationshipImportResult.builder()
            .direction(direction)
            .topology(relationships.topology())
            .properties(
                propertySchema.map(schema -> {
                    var relationshipProperty = ImmutableRelationshipProperty.builder()
                        .values(relationships.properties().orElseThrow(IllegalStateException::new))
                        .propertySchema(schema)
                        .build();
                    return RelationshipPropertyStore
                        .builder()
                        .putRelationshipProperty(schema.key(), relationshipProperty)
                        .build();
                })).build();
    }
}
