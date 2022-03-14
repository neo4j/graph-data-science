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

import org.eclipse.collections.api.IntIterable;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.collections.HugeSparseLongList;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.UpdatableNodeProperty;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;

import java.util.HashMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class InMemoryTransactionStateVisitor extends TxStateVisitor.Adapter {

    private final CypherGraphStore graphStore;
    private final TokenHolders tokenHolders;

    public InMemoryTransactionStateVisitor(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders
    ) {
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void visitNodePropertyChanges(
        long nodeId, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed
    ) {
        var addedOrChangedProperties = Iterables.concat(added, changed);
        for (StorageProperty storageProperty : addedOrChangedProperties) {
            try {
                var propertyName = tokenHolders.propertyKeyTokens().getTokenById(storageProperty.propertyKeyId()).name();
                var propertyValue = storageProperty.value();

                graphStore.nodes().forEachNodeLabel(nodeId, nodeLabel -> {
                    graphStore.updatableNodeProperties()
                        .computeIfAbsent(nodeLabel, __ -> new HashMap<>())
                        .computeIfAbsent(propertyName, __ -> updatableNodePropertyFromValue(propertyValue))
                        .updatePropertyValue(nodeId, propertyValue);

                    return true;
                });
            } catch (TokenNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private UpdatableNodeProperty updatableNodePropertyFromValue(Value value) {
        if (value instanceof LongValue) {
            var hugeSparseLongList = HugeSparseLongList.of(DefaultValue.forLong().longValue());
            return new UpdatableNodeProperty.UpdatableLongNodeProperty() {
                @Override
                public long size() {
                    return graphStore.nodeCount();
                }

                @Override
                public long longValue(long nodeId) {
                    return hugeSparseLongList.get(nodeId);
                }

                @Override
                public void updatePropertyValue(long nodeId, Value value) {
                    hugeSparseLongList.set(nodeId, ((LongValue) value).longValue());
                }
            };
        }
        throw new IllegalArgumentException(formatWithLocale("Unsupported property type %s", value.getTypeName()));
    }
}
