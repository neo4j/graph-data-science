/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;

public final class IndexedNodePropertyImporter extends StatementAction {
    private final NodeLabel nodeLabel;
    private final PropertyMapping mapping;
    private final IndexDescriptor index;
    private final int propertyId;
    private final IdMapping idMap;
    private final ProgressLogger progressLogger;
    private final TerminationFlag terminationFlag;
    private final NodePropertiesBuilder propertiesBuilder;
    private long imported;
    private long logged;

    IndexedNodePropertyImporter(
        SecureTransaction tx,
        NodeLabel nodeLabel,
        PropertyMapping mapping,
        IndexDescriptor index,
        IdMapping idMap,
        int concurrency,
        ProgressLogger progressLogger,
        TerminationFlag terminationFlag,
        AllocationTracker tracker
    ) {
        super(tx);
        this.nodeLabel = nodeLabel;
        this.mapping = mapping;
        this.index = index;
        propertyId = index.schema().getPropertyId();
        this.idMap = idMap;
        this.progressLogger = progressLogger;
        this.terminationFlag = terminationFlag;
        propertiesBuilder = NodePropertiesBuilder.of(
            idMap.nodeCount(),
            tracker,
            mapping.defaultValue(),
            propertyId,
            mapping.propertyKey(),
            concurrency
        );
    }

    @Override
    public String threadName() {
        return "index-scan-" + index.getName();
    }

    @Override
    public void accept(KernelTransaction ktx) throws Exception {
        var read = ktx.dataRead();
        try (var indexCursor = ktx.cursors().allocateNodeValueIndexCursor()) {
            var indexReadSession = read.indexReadSession(index);
            read.nodeIndexScan(indexReadSession, indexCursor, IndexOrder.NONE, true);
            while (indexCursor.next()) {
                if (indexCursor.hasValue()) {
                    var node = indexCursor.nodeReference();
                    var numberOfProperties = indexCursor.numberOfProperties();
                    for (int i = 0; i < numberOfProperties; i++) {
                        var propertyKey = indexCursor.propertyKey(i);
                        if (propertyId == propertyKey) {
                            var propertyValue = indexCursor.propertyValue(i);
                            var value = ReadHelper.extractValue(propertyValue, mapping.defaultValue());
                            var nodeId = idMap.toMappedNodeId(node);
                            propertiesBuilder.set(nodeId, value);
                            imported += 1;
                            if ((imported & 0x1_FFFFL) == 0L) {
                                progressLogger.logProgress(imported - logged);
                                logged = imported;
                                terminationFlag.assertRunning();
                            }
                        }
                    }
                }
            }
        }
    }

    NodeLabel nodeLabel() {
        return nodeLabel;
    }

    PropertyMapping mapping() {
        return mapping;
    }

    long imported() {
        return imported;
    }

    NodeProperties build() {
        return propertiesBuilder.build();
    }
}
