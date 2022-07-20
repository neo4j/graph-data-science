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
package org.neo4j.gds.storageengine;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.UpdatableNodeProperty;
import org.neo4j.gds.core.cypher.nodeproperties.UpdatableDoubleArrayNodeProperty;
import org.neo4j.gds.core.cypher.nodeproperties.UpdatableDoubleNodeProperty;
import org.neo4j.gds.core.cypher.nodeproperties.UpdatableFloatArrayNodeProperty;
import org.neo4j.gds.core.cypher.nodeproperties.UpdatableLongArrayNodeProperty;
import org.neo4j.gds.core.cypher.nodeproperties.UpdatableLongNodeProperty;
import org.neo4j.gds.core.loading.ValueConverter;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import java.util.Iterator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class InMemoryTransactionStateVisitor extends TxStateVisitor.Adapter {

    private final CypherGraphStore graphStore;
    private final TokenHolders tokenHolders;
    private final IntObjectMap<UpdatableNodeProperty> nodePropertiesCache;

    public InMemoryTransactionStateVisitor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        this.nodePropertiesCache = new IntObjectHashMap<>();
    }

    // Neo4j >= 4.4
    public void visitNodePropertyChanges(
        long nodeId, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed
    ) {
        visitNodePropertyChanges(nodeId, added.iterator(), changed.iterator(), removed);
    }

    // Neo4j <= 4.3
    public void visitNodePropertyChanges(
        long nodeId, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed
    ) {
        if (!removed.isEmpty()) {
            throw new UnsupportedOperationException(
                "Dropping single node properties is not supported. Use the `gds.graph.nodeProperties.drop` procedure " +
                "instead to drop the entire property from the graph."
            );
        }
        visitAddedOrChangedNodeProperties(nodeId, added, changed);
    }

    public void removeNodeProperty(String propertyKey) {
        var propertyToken = tokenHolders.propertyKeyTokens().getIdByName(propertyKey);
        var usedByOtherLabels = graphStore.nodeLabels().stream().anyMatch(label -> graphStore.hasNodeProperty(label, propertyKey));
        if (!usedByOtherLabels) {
            this.nodePropertiesCache.remove(propertyToken);
        }
    }

    @Override
    public void visitNodeLabelChanges(
        long id, LongSet added, LongSet removed
    ) {
        added.forEach(addedLabelToken -> {
            try {
                var labelName = tokenHolders.labelTokens().getTokenById((int) addedLabelToken).name();
                graphStore.addLabelToNode(id, NodeLabel.of(labelName));
            } catch (TokenNotFoundException e) {
                throw new RuntimeException(e);
            }
        });

        removed.forEach(removedLabelToken -> {
            try {
                var labelName = tokenHolders.labelTokens().getTokenById((int) removedLabelToken).name();
                graphStore.removeLabelFromNode(id, NodeLabel.of(labelName));
            } catch (TokenNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void visitCreatedLabelToken(long id, String name, boolean internal) {
        graphStore.addNodeLabel(NodeLabel.of(name));
    }

    private void visitAddedOrChangedNodeProperties(long nodeId, Iterator<StorageProperty> added, Iterator<StorageProperty> changed) {
        var addedOrChangedProperties = Iterators.concat(added, changed);

        addedOrChangedProperties.forEachRemaining(storageProperty -> {
            var propertyKeyId = storageProperty.propertyKeyId();
            var propertyKey = tokenHolders.propertyKeyGetName(propertyKeyId);
            var propertyValue = storageProperty.value();

            UpdatableNodeProperty nodeProperties;
            if (this.nodePropertiesCache.containsKey(propertyKeyId)) {
                nodeProperties = this.nodePropertiesCache.get(propertyKeyId);
            } else {
                nodeProperties = createUpdatableNodeProperty(propertyKeyId, propertyValue);
                graphStore.addNodeProperty(graphStore.nodeLabels(), propertyKey, nodeProperties);
            }
            nodeProperties.updatePropertyValue(nodeId, propertyValue);
        });
    }

    private UpdatableNodeProperty createUpdatableNodeProperty(int propertyKeyToken, Value value) {
        var updatableNodeProperty = updatableNodePropertyFromValue(value);
        this.nodePropertiesCache.put(propertyKeyToken, updatableNodeProperty);
        return updatableNodeProperty;
    }

    private UpdatableNodeProperty updatableNodePropertyFromValue(Value value) {
        var valueType = ValueConverter.valueType(value);
        var defaultValue = valueType.fallbackValue();
        var nodeCount = graphStore.nodeCount();

        switch (valueType) {
            case LONG:
                return new UpdatableLongNodeProperty(nodeCount, defaultValue.longValue());
            case DOUBLE:
                return new UpdatableDoubleNodeProperty(nodeCount, defaultValue.doubleValue());
            case LONG_ARRAY:
                return new UpdatableLongArrayNodeProperty(nodeCount, defaultValue.longArrayValue());
            case DOUBLE_ARRAY:
                return new UpdatableDoubleArrayNodeProperty(nodeCount, defaultValue.doubleArrayValue());
            case FLOAT_ARRAY:
                return new UpdatableFloatArrayNodeProperty(nodeCount, defaultValue.floatArrayValue());
            default:
                throw new IllegalArgumentException(formatWithLocale("Unsupported property type %s", value.getTypeName()));
        }
    }
}
