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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

public abstract class AbstractInMemoryRelationshipPropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor {

    private String currentRelationshipPropertyKey = null;

    private final Map<String, ValueGroup> propertyKeyToTypeMapping;
    private final Set<Integer> seenRelationshipReferences;

    protected PropertySelection propertySelection;

    protected AbstractInMemoryRelationshipPropertyCursor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        super(NO_ID, graphStore, tokenHolders);
        this.seenRelationshipReferences = new HashSet<>();
        this.propertyKeyToTypeMapping = new HashMap<>();
        var propertySchemas = graphStore
            .schema()
            .relationshipSchema()
            .properties()
            .values();
        graphStore.relationshipPropertyKeys(graphStore.relationshipTypes()).forEach(nodePropertyKey -> {
            var valueType = propertySchemas
                .stream()
                .map(map -> map.get(nodePropertyKey))
                .findFirst()
                .get()
                .valueType();

            var valueGroup = valueGroupFromValueType(valueType);

            propertyKeyToTypeMapping.put(nodePropertyKey, valueGroup);
        });
    }

    @Override
    public int propertyKey() {
        return tokenHolders.propertyKeyTokens().getIdByName(currentRelationshipPropertyKey);
    }

    @Override
    public ValueGroup propertyType() {
        return propertyKeyToTypeMapping.get(currentRelationshipPropertyKey);
    }

    @Override
    public Value propertyValue() {
        if (currentRelationshipPropertyKey != null) {
            return Values.doubleValue(graphStore.relationshipIds().propertyValueForId(getId(),
                currentRelationshipPropertyKey
            ));
        }
        throw new IllegalStateException(
            "Property cursor is initialized as node and relationship cursor, maybe you forgot to `reset()`?");
    }

    @Override
    public boolean next() {
        if (getId() != NO_ID) {
            Optional<NamedToken> maybeNextEntry = StreamSupport.stream(tokenHolders
                    .propertyKeyTokens()
                    .getAllTokens()
                    .spliterator(), false)
                .filter(tokenHolder -> propertySelection.test(tokenHolder.id()) && !seenRelationshipReferences.contains(tokenHolder.id()))
                .findFirst();

            if (maybeNextEntry.isPresent()) {
                currentRelationshipPropertyKey = maybeNextEntry.get().name();
                seenRelationshipReferences.add(maybeNextEntry.get().id());
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        clear();
        this.setId(NO_ID);
        this.currentRelationshipPropertyKey = null;
        this.seenRelationshipReferences.clear();
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }

    private static ValueGroup valueGroupFromValueType(ValueType valueType) {
        switch (valueType) {
            case DOUBLE:
            case LONG:
                return ValueGroup.NUMBER;
            case LONG_ARRAY:
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return ValueGroup.NUMBER_ARRAY;
            default:
                return ValueGroup.UNKNOWN;
        }
    }
}
