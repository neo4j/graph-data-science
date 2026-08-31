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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.properties.relationships.RelationshipPropertyStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.MutableRelationshipSchemaEntry;

import java.util.Optional;

@GenerateBuilder
public record SingleTypeRelationships(
    Topology topology,
    MutableRelationshipSchemaEntry relationshipSchemaEntry,
    Optional<RelationshipPropertyStore> properties,
    Optional<Topology> inverseTopology,
    Optional<RelationshipPropertyStore> inverseProperties
) {

    public static final SingleTypeRelationships EMPTY = new SingleTypeRelationships(
        Topology.EMPTY,
        new MutableRelationshipSchemaEntry(RelationshipType.of("REL"), Direction.DIRECTED)
    );

    public SingleTypeRelationships {
        if (properties.map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            properties = Optional.empty();
        }
        if (inverseProperties.map(RelationshipPropertyStore::isEmpty).orElse(false)) {
            inverseProperties = Optional.empty();
        }
    }

    public SingleTypeRelationships(Topology topology, MutableRelationshipSchemaEntry relationshipSchemaEntry) {
        this(topology, relationshipSchemaEntry, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public SingleTypeRelationships(Topology topology, MutableRelationshipSchemaEntry relationshipSchemaEntry, RelationshipPropertyStore properties) {
        this(topology, relationshipSchemaEntry, Optional.of(properties), Optional.empty(), Optional.empty());
    }

    public long count() {
        return topology.elementCount();
    }

    public AdjacencyList adjacencyList() {
        return topology.adjacencyList();
    }
}
