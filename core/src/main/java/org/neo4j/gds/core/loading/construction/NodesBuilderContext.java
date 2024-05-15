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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;

abstract class NodesBuilderContext {

    private static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;

    // Thread-local mappings that can be computed independently.
    private final Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier;
    private final Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier;
    // Thread-global mapping as all threads need to write to the same property builders.
    private final Set<NodeLabelTokenToPropertyKeys> threadLocalNodeLabelTokenToPropertyKeys;

    protected final Map<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder;
    protected final Concurrency concurrency;

    /**
     * Used if no node schema information is available and needs to be inferred from the input data.
     */
    static NodesBuilderContext lazy(Concurrency concurrency) {
        return new Lazy(
            TokenToNodeLabels::lazy,
            NodeLabelTokenToPropertyKeys::lazy,
            new ConcurrentHashMap<>(),
            concurrency
        );
    }

    /**
     * Used if a node schema is available upfront.
     */
    static NodesBuilderContext fixed(NodeSchema nodeSchema, Concurrency concurrency) {
        var propertyBuildersByPropertyKey = nodeSchema.unionProperties().entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> NodePropertiesFromStoreBuilder.of(e.getValue().defaultValue(), concurrency)
        ));

        return new Fixed(
            () -> TokenToNodeLabels.fixed(nodeSchema.availableLabels()),
            () -> NodeLabelTokenToPropertyKeys.fixed(nodeSchema),
            new HashMap<>(propertyBuildersByPropertyKey),
            concurrency
        );
    }

    Map<String, NodePropertiesFromStoreBuilder> nodePropertyBuilders() {
        return this.propertyKeyToPropertyBuilder;
    }

    Collection<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeys() {
        return threadLocalNodeLabelTokenToPropertyKeys;
    }

    private NodesBuilderContext(
        Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier,
        Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier,
        Map<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder,
        Concurrency concurrency
    ) {
        this.tokenToNodeLabelSupplier = tokenToNodeLabelSupplier;
        this.nodeLabelTokenToPropertyKeysSupplier = nodeLabelTokenToPropertyKeysSupplier;
        this.propertyKeyToPropertyBuilder = propertyKeyToPropertyBuilder;
        this.concurrency = concurrency;
        this.threadLocalNodeLabelTokenToPropertyKeys = ConcurrentHashMap.newKeySet();
    }

    ThreadLocalContext threadLocalContext() {
        NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys = nodeLabelTokenToPropertyKeysSupplier.get();
        threadLocalNodeLabelTokenToPropertyKeys.add(nodeLabelTokenToPropertyKeys);

        return new ThreadLocalContext(
            tokenToNodeLabelSupplier.get(),
            nodeLabelTokenToPropertyKeys,
            this::getPropertyBuilder
        );
    }

    abstract NodePropertiesFromStoreBuilder getPropertyBuilder(String propertyKey);

    private static final class Fixed extends NodesBuilderContext {
        Fixed(
            Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier,
            Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier,
            Map<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder,
            Concurrency concurrency
        ) {
            super(tokenToNodeLabelSupplier, nodeLabelTokenToPropertyKeysSupplier, propertyKeyToPropertyBuilder, concurrency);
        }

        @Override
        NodePropertiesFromStoreBuilder getPropertyBuilder(String propertyKey) {
            return propertyKeyToPropertyBuilder.get(propertyKey);
        }
    }

    private static final class Lazy extends NodesBuilderContext {
        Lazy(
            Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier,
            Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier,
            ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyKeyToPropertyBuilder,
            Concurrency concurrency
        ) {
            super(tokenToNodeLabelSupplier, nodeLabelTokenToPropertyKeysSupplier, propertyKeyToPropertyBuilder, concurrency);
        }

        @Override
        NodePropertiesFromStoreBuilder getPropertyBuilder(String propertyKey) {
            return this.propertyKeyToPropertyBuilder.computeIfAbsent(
                propertyKey,
                __ -> NodePropertiesFromStoreBuilder.of(NO_PROPERTY_VALUE, this.concurrency)
            );
        }
    }

    static class ThreadLocalContext {

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

        IntObjectMap<List<NodeLabel>> threadLocalTokenToNodeLabels() {
            return this.tokenToNodeLabels.labelTokenNodeLabelMapping();
        }

        NodeLabelTokenSet addNodeLabelToken(NodeLabelToken nodeLabelToken) {
            return getOrCreateLabelTokens(nodeLabelToken);
        }

        NodeLabelTokenSet addNodeLabelTokenAndPropertyKeys(NodeLabelToken nodeLabelToken, Iterable<String> propertyKeys) {
            NodeLabelTokenSet tokens = getOrCreateLabelTokens(nodeLabelToken);
            this.nodeLabelTokenToPropertyKeys.add(nodeLabelToken, propertyKeys);
            return tokens;
        }

        private NodeLabelTokenSet getOrCreateLabelTokens(NodeLabelToken nodeLabelToken) {
            if (nodeLabelToken.isEmpty()) {
                return anyLabelArray();
            }

            int[] labelIds = new int[nodeLabelToken.size()];
            for (int i = 0; i < labelIds.length; i++) {
                labelIds[i] = this.tokenToNodeLabels.getOrCreateToken(nodeLabelToken.get(i));
            }

            return NodeLabelTokenSet.from(labelIds);
        }

        private NodeLabelTokenSet anyLabelArray() {
            var token = tokenToNodeLabels.getOrCreateToken(NodeLabel.ALL_NODES);
            return NodeLabelTokenSet.from(token);
        }
    }
}
