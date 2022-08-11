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
package org.neo4j.gds.core.io.schema;

import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;

public class RelationshipSchemaBuilderVisitor extends RelationshipSchemaVisitor {

    // Graphs read from CSV are assumed to be always directed
    private static final boolean IS_UNDIRECTED = false;
    private final RelationshipSchema.Builder schemaBuilder;
    private RelationshipSchema schema;

    public RelationshipSchemaBuilderVisitor() {
        schemaBuilder = RelationshipSchema.builder();
    }

    @Override
    protected void export() {
        // If the key is null we expect no properties but a relationship type
        if (key() == null) {
            schemaBuilder.addRelationshipType(relationshipType(), IS_UNDIRECTED);
        } else {
            schemaBuilder.addProperty(
                relationshipType(),
                IS_UNDIRECTED,
                key(),
                RelationshipPropertySchema.of(key(), valueType(), defaultValue(), state(), aggregation())
            );
        }
    }

    public RelationshipSchema schema() {
        return schema;
    }

    @Override
    public void close() {
        schema = schemaBuilder.build();
    }
}
