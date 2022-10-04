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

import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractInMemoryNodePropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor {

    private final int[] nodePropertyKeyMapping;
    private final Value[] nodePropertyValues;
    private final CypherGraphStore graphStore;
    private final TokenHolders tokenHolders;
    private final Map<Integer, ValueGroup> propertyKeyToValueGroupMapping;

    private int nodePropertyCount;
    private int currentNodeProperty;

    public AbstractInMemoryNodePropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);

        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        int propertyCount = graphStore.nodePropertyKeys().size();
        this.nodePropertyKeyMapping = new int[propertyCount];
        this.nodePropertyValues = new Value[propertyCount];
        this.nodePropertyCount = 0;
        this.currentNodeProperty = -1;
        this.propertyKeyToValueGroupMapping = new HashMap<>();
        populateKeyToValueGroupMapping();
    }

    @Override
    public int propertyKey() {
        if (currentNodeProperty >= 0) {
            return nodePropertyKeyMapping[currentNodeProperty];
        } else {
            throw new IllegalStateException(
                "Property cursor is initialized as node and relationship cursor, maybe you forgot to `reset()`?");
        }
    }

    protected void setPropertySelection(InMemoryPropertySelection propertySelection) {
        var nodeId = getId();
        this.nodePropertyCount = 0;

        graphStore.nodes().forEachNodeLabel(nodeId, label -> {
            for (String nodePropertyKey : graphStore.nodePropertyKeys(label)) {
                int propertyId = tokenHolders.propertyKeyTokens().getIdByName(nodePropertyKey);
                if (propertySelection.test(propertyId) && !ArrayUtil.linearSearch(nodePropertyKeyMapping, nodePropertyKeyMapping.length, propertyId)) {
                    int propertyIndex = nodePropertyCount++;
                    nodePropertyKeyMapping[propertyIndex] = propertyId;

                    if (!propertySelection.isKeysOnly()) {
                        nodePropertyValues[propertyIndex] = graphStore
                            .nodeProperty(nodePropertyKey)
                            .values()
                            .value(nodeId);
                    }

                    if (propertySelection.isLimited() && nodePropertyCount == propertySelection.numberOfKeys()) {
                        return false;
                    }
                }
            }
            return true;
        });
    }

    @Override
    public boolean next() {
        if (getId() != NO_ID) {
            currentNodeProperty++;
            return currentNodeProperty < nodePropertyCount;
        }
        return false;
    }

    @Override
    public Value propertyValue() {
        if (currentNodeProperty >= 0) {
            return nodePropertyValues[currentNodeProperty];
        } else {
            throw new IllegalStateException(
                "Property cursor is initialized as node and relationship cursor, maybe you forgot to `reset()`?");
        }
    }

    @Override
    public void reset() {
        clear();
        this.setId(NO_ID);
        this.nodePropertyCount = 0;
        this.currentNodeProperty = -1;
        Arrays.fill(this.nodePropertyKeyMapping, -1);
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }

    @Override
    public ValueGroup propertyType() {
        return propertyKeyToValueGroupMapping.get(propertyKey());
    }


    private void populateKeyToValueGroupMapping() {
        graphStore.schema().nodeSchema().properties()
            .forEach((identifier, propertyMap) ->
                propertyMap.forEach((propertyKey, propertySchema) ->
                    this.propertyKeyToValueGroupMapping.put(
                        tokenHolders.propertyKeyTokens().getIdByName(propertyKey),
                        valueGroupFromValueType(propertySchema.valueType())
                    )
                )
            );
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
