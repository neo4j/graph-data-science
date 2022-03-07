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

import org.bouncycastle.util.Arrays;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractInMemoryNodePropertyCursor extends AbstractInMemoryPropertyCursor.DelegatePropertyCursor<NodeLabel, PropertySchema> {

    private final int[] nodePropertyKeyMapping;
    private final Value[] nodePropertyValues;

    private int nodePropertyCount;
    private int currentNodeProperty;

    public AbstractInMemoryNodePropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID, graphStore, tokenHolders);

        int propertyCount = graphStore.nodePropertyKeys().size();
        this.nodePropertyKeyMapping = new int[propertyCount];
        this.nodePropertyValues = new Value[propertyCount];
        this.nodePropertyCount = 0;
        this.currentNodeProperty = -1;
    }

    @Override
    protected Map<NodeLabel, Map<String, PropertySchema>> propertySchema() {
        return graphStore.schema().nodeSchema().properties();
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

    @Override
    protected void setPropertySelection(Predicate<Integer> propertySelection) {
        var nodeId = getId();
        this.nodePropertyCount = 0;

        graphStore.nodes().forEachNodeLabel(nodeId, label -> {
            for (String nodePropertyKey : graphStore.nodePropertyKeys(label)) {
                int propertyId = tokenHolders.propertyKeyTokens().getIdByName(nodePropertyKey);
                if (propertySelection.test(propertyId) && !Arrays.contains(nodePropertyKeyMapping, propertyId)) {
                    int propertyIndex = nodePropertyCount++;
                    nodePropertyKeyMapping[propertyIndex] = propertyId;
                    nodePropertyValues[propertyIndex] = graphStore
                        .nodePropertyValues(label, nodePropertyKey)
                        .value(nodeId);
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
}
