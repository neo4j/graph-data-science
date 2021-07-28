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
package org.neo4j.graphalgo.core.utils.io;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.TransactionContext;

import java.util.function.LongFunction;

final class NeoStoreProperties implements LongFunction<Object> {

    final TransactionContext transactionContext;
    final NodeMapping nodeMapping;
    private final PropertyMapping propertyMapping;

    static LongFunction<Object> of(
        TransactionContext transactionContext,
        NodeMapping nodeMapping,
        PropertyMapping propertyMapping
    ) {
        return new NeoStoreProperties(transactionContext, nodeMapping, propertyMapping);
    }

    private NeoStoreProperties(
        TransactionContext transactionContext,
        NodeMapping nodeMapping,
        PropertyMapping propertyMapping
    ) {
        this.transactionContext = transactionContext;
        this.nodeMapping = nodeMapping;
        this.propertyMapping = propertyMapping;
    }

    @Override
    public Object apply(long nodeId) {
        long originalId = nodeMapping.toOriginalNodeId(nodeId);

        return transactionContext.apply((tx, ktx) -> tx
            .getNodeById(originalId)
            .getProperty(propertyMapping.neoPropertyKey(), propertyMapping.defaultValue().getObject()));
    }
}
