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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.ImmutableRelationshipProperty;
import org.neo4j.gds.api.Properties;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchemaEntry;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchemaEntry;

import java.util.Optional;

@ValueClass
public interface SingleTypeRelationships {

    SingleTypeRelationships EMPTY = SingleTypeRelationships
            .builder()
            .direction(Direction.DIRECTED)
            .topology(Topology.EMPTY)
            .build();

    // TODO: figure out if we can remove this.
    Direction direction();

    Topology topology();

    RelationshipSchemaEntry relationshipSchemaEntry();

    Optional<RelationshipPropertyStore> properties();

    Optional<Topology> inverseTopology();

    Optional<RelationshipPropertyStore> inverseProperties();

    /**
     * Filters the relationships to include only the given property if present.
     */
    default SingleTypeRelationships filter(String propertyKey) {
        var properties = properties().map(relationshipPropertyStore ->
            relationshipPropertyStore.filter(propertyKey)
        );
        var inverseProperties = inverseProperties().map(relationshipPropertyStore ->
            relationshipPropertyStore.filter(propertyKey)
        );


        RelationshipSchemaEntry entry = relationshipSchemaEntry();
        var filteredEntry = new RelationshipSchemaEntry(entry.identifier(), entry.direction())
            .addProperty(propertyKey, entry.properties().get(propertyKey));

        return SingleTypeRelationships.builder()
            .topology(topology())
            .relationshipSchemaEntry(filteredEntry)
            .direction(direction())
            .inverseTopology(inverseTopology())
            .properties(properties)
            .inverseProperties(inverseProperties)
            .build();
    }

    @Value.Check
    default SingleTypeRelationships normalize() {
        if (properties().map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            return builder().from(this).properties(Optional.empty()).build();
        }
        if (inverseProperties().map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            return builder().from(this).inverseProperties(Optional.empty()).build();
        }
        return this;
    }

    static ImmutableSingleTypeRelationships.Builder builder() {
        return ImmutableSingleTypeRelationships.builder();
    }

    static SingleTypeRelationships of(
        RelationshipType relationshipType,
        Topology topology,
        Direction direction,
        Optional<Properties> properties,
        Optional<RelationshipPropertySchema> propertySchema
    ) {
        var schemaEntry = new RelationshipSchemaEntry(relationshipType, direction);
        propertySchema.ifPresent(schema -> schemaEntry.addProperty(schema.key(), schema));

        return SingleTypeRelationships.builder()
            .direction(direction)
            .topology(topology)
            .relationshipSchemaEntry(schemaEntry)
            .properties(
                propertySchema.map(schema -> {
                    var relationshipProperty = ImmutableRelationshipProperty.builder()
                        .values(properties.orElseThrow(IllegalStateException::new))
                        .propertySchema(schema)
                        .build();
                    return RelationshipPropertyStore
                        .builder()
                        .putRelationshipProperty(schema.key(), relationshipProperty)
                        .build();
                })).build();
    }
}
