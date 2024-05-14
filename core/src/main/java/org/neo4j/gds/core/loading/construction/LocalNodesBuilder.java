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

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.NodeImporter;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.gds.core.loading.NodesBatchBuffer;
import org.neo4j.gds.core.loading.NodesBatchBufferBuilder;
import org.neo4j.gds.core.utils.RawValues;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongPredicate;

import static org.neo4j.gds.core.loading.construction.NodesBuilder.NO_PROPERTY;

final class LocalNodesBuilder implements AutoCloseable {

    private final LongAdder importedNodes;
    private final LongPredicate seenNodeIdPredicate;
    private final NodesBatchBuffer<Integer> buffer;
    private final NodeImporter nodeImporter;
    private final List<PropertyValues> batchNodeProperties;
    private final NodesBuilderContext.ThreadLocalContext threadLocalContext;

    LocalNodesBuilder(
        LongAdder importedNodes,
        NodeImporter nodeImporter,
        LongPredicate seenNodeIdPredicate,
        boolean hasLabelInformation,
        boolean hasProperties,
        NodesBuilderContext.ThreadLocalContext threadLocalContext
    ) {
        this.importedNodes = importedNodes;
        this.seenNodeIdPredicate = seenNodeIdPredicate;
        this.threadLocalContext = threadLocalContext;

        this.buffer = new NodesBatchBufferBuilder<Integer>()
            .capacity(ParallelUtil.DEFAULT_BATCH_SIZE)
            .hasLabelInformation(hasLabelInformation)
            .readProperty(hasProperties)
            .propertyReferenceClass(Integer.class)
            .build();

        this.nodeImporter = nodeImporter;
        this.batchNodeProperties = new ArrayList<>(buffer.capacity());
    }

    public void addNode(long originalId, NodeLabelToken nodeLabelToken) {
        if (!seenNodeIdPredicate.test(originalId)) {
            var threadLocalTokens = threadLocalContext.addNodeLabelToken(nodeLabelToken);

            buffer.add(originalId, NO_PROPERTY, threadLocalTokens);
            if (buffer.isFull()) {
                flushBuffer();
                reset();
            }
        }
    }

    public void addNode(long originalId, NodeLabelToken nodeLabelToken, PropertyValues properties) {
        if (!seenNodeIdPredicate.test(originalId)) {
            var threadLocalTokens = threadLocalContext.addNodeLabelTokenAndPropertyKeys(
                nodeLabelToken,
                properties.propertyKeys()
            );
            int propertyReference = batchNodeProperties.size();
            batchNodeProperties.add(properties);

            buffer.add(originalId, propertyReference, threadLocalTokens);
            if (buffer.isFull()) {
                flushBuffer();
                reset();
            }
        }
    }

    public void flush() {
        flushBuffer();
        reset();
    }

    private void reset() {
        buffer.reset();
        batchNodeProperties.clear();
    }

    private void flushBuffer() {
        var importedNodesAndProperties = this.nodeImporter.importNodes(
            this.buffer,
            this.threadLocalContext.threadLocalTokenToNodeLabels(),
            this::importProperties
        );
        int importedNodes = RawValues.getHead(importedNodesAndProperties);
        this.importedNodes.add(importedNodes);
    }

    private int importProperties(long nodeReference, NodeLabelTokenSet labelTokens, int propertyValueIndex) {
        if (propertyValueIndex != NO_PROPERTY) {
            var properties = this.batchNodeProperties.get(propertyValueIndex);

            properties.forEach((propertyKey, propertyValue) -> {
                var nodePropertyBuilder = this.threadLocalContext.nodePropertyBuilder(propertyKey);
                assert nodePropertyBuilder != null : "observed property key that is not present in schema";
                nodePropertyBuilder.set(nodeReference, propertyValue);

            });
            return properties.size();
        }
        return 0;
    }

    @Override
    public void close() {}

    NodesBuilderContext.ThreadLocalContext threadLocalContext() {
        return threadLocalContext;
    }
}
