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
package org.neo4j.gds.compat;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Map;

public abstract class AbstractInMemoryRelationshipPropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor<RelationshipType, RelationshipPropertySchema> {

    protected AbstractInMemoryRelationshipPropertyCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        super(NO_ID, graphStore, tokenHolders);
    }

    @Override
    protected Map<RelationshipType, Map<String, RelationshipPropertySchema>> propertySchema() {
        return graphStore.schema().relationshipSchema().properties();
    }

    @Override
    public Value propertyValue() {
        if (currentPropertyKey != null) {
            return Values.doubleValue(graphStore.relationshipIds().propertyValueForId(getId(),
                currentPropertyKey
            ));
        }
        throw new IllegalStateException(
            "Property cursor is initialized as node and relationship cursor, maybe you forgot to `reset()`?");
    }
}
