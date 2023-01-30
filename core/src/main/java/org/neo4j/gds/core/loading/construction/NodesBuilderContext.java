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
package org.neo4j.gds.core.loading.construction;

import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;

final class NodesBuilderContext {

    private static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;

    // Thread-local mappings that can be computed independently.
    private final Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier;
    private final Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier;
    // Thread-global mapping as all threads need to write to the same property builders.
    private final ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder;

    private final int concurrency;

    /**
     * Used if no node schema information is available and needs to be inferred from the input data.
     */
    static NodesBuilderContext lazy(int concurrency) {
        return new NodesBuilderContext(
            TokenToNodeLabels::lazy,
            NodeLabelTokenToPropertyKeys::lazy,
            new ConcurrentHashMap<>(),
            concurrency
        );
    }

    /**
     * Used if a node schema is available upfront.
     */
    static NodesBuilderContext fixed(NodeSchema nodeSchema, int concurrency) {
        var propertyBuildersByPropertyKey = nodeSchema.unionProperties().entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> NodePropertiesFromStoreBuilder.of(e.getValue().defaultValue(), concurrency)
        ));

        return new NodesBuilderContext(
            () -> TokenToNodeLabels.fixed(nodeSchema.availableLabels()),
            () -> NodeLabelTokenToPropertyKeys.fixed(nodeSchema),
            new ConcurrentHashMap<>(propertyBuildersByPropertyKey),
            concurrency
        );
    }

    Map<String, NodePropertiesFromStoreBuilder> nodePropertyBuilders() {
        return this.propertyKeyToPropertyBuilder;
    }

    private NodesBuilderContext(
        Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier,
        Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier,
        ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder,
        int concurrency
    ) {
        this.tokenToNodeLabelSupplier = tokenToNodeLabelSupplier;
        this.nodeLabelTokenToPropertyKeysSupplier = nodeLabelTokenToPropertyKeysSupplier;
        this.propertyKeyToPropertyBuilder = propertyKeyToPropertyBuilder;
        this.concurrency = concurrency;
    }

    ThreadLocalContext threadLocalContext() {
        Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn = this.propertyKeyToPropertyBuilder.isEmpty()
            ? this::getOrCreatePropertyBuilder
            : this::getPropertyBuilder;

        return new ThreadLocalContext(
            tokenToNodeLabelSupplier.get(),
            nodeLabelTokenToPropertyKeysSupplier.get(),
            propertyBuilderFn
        );
    }

    private NodePropertiesFromStoreBuilder getOrCreatePropertyBuilder(String propertyKey) {
        return this.propertyKeyToPropertyBuilder.computeIfAbsent(
            propertyKey,
            __ -> NodePropertiesFromStoreBuilder.of(NO_PROPERTY_VALUE, concurrency)
        );
    }

    private NodePropertiesFromStoreBuilder getPropertyBuilder(String propertyKey) {
        return this.propertyKeyToPropertyBuilder.get(propertyKey);
    }

    static class ThreadLocalContext {

        private static final long NOT_INITIALIZED = -42L;

        private final long[] anyLabelArray = {NOT_INITIALIZED};
        private final TokenToNodeLabels tokenToNodeLabels;
        private final NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys;
        private final Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn;

        ThreadLocalContext(
            TokenToNodeLabels tokenToNodeLabels,
            NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys,
            Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn
        ) {
            this.tokenToNodeLabels = tokenToNodeLabels;
            this.nodeLabelTokenToPropertyKeys = nodeLabelTokenToPropertyKeys;
            this.propertyBuilderFn = propertyBuilderFn;
        }

        NodePropertiesFromStoreBuilder nodePropertyBuilder(String propertyKey) {
            return this.propertyBuilderFn.apply(propertyKey);
        }

        NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys() {
            return this.nodeLabelTokenToPropertyKeys;
        }

        IntObjectMap<List<NodeLabel>> threadLocalTokenToNodeLabels() {
            return this.tokenToNodeLabels.labelTokenNodeLabelMapping();
        }

        long[] addNodeLabelToken(NodeLabelToken nodeLabelToken) {
            return getOrCreateLabelTokens(nodeLabelToken);
        }

        long[] addNodeLabelTokenAndPropertyKeys(NodeLabelToken nodeLabelToken, Iterable<String> propertyKeys) {
            long[] tokens = getOrCreateLabelTokens(nodeLabelToken);
            this.nodeLabelTokenToPropertyKeys.add(nodeLabelToken, propertyKeys);
            return tokens;
        }

        private long[] getOrCreateLabelTokens(NodeLabelToken nodeLabelToken) {
            if (nodeLabelToken.isEmpty()) {
                return anyLabelArray();
            }

            long[] labelIds = new long[nodeLabelToken.size()];
            for (int i = 0; i < labelIds.length; i++) {
                labelIds[i] = this.tokenToNodeLabels.getOrCreateToken(nodeLabelToken.get(i));
            }

            return labelIds;
        }

        private long[] anyLabelArray() {
            var anyLabelArray = this.anyLabelArray;
            if (anyLabelArray[0] == NOT_INITIALIZED) {
                anyLabelArray[0] = tokenToNodeLabels.getOrCreateToken(NodeLabel.ALL_NODES);
            }
            return anyLabelArray;
        }
    }
}
