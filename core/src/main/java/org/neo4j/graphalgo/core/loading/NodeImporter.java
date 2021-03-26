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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.IntObjectMap;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NodeImporter {

    public interface PropertyReader {
        int readProperty(long nodeReference, long[] labelIds, long propertiesReference, long internalId);
    }

    private final InternalIdMappingBuilder<? extends IdMappingAllocator> idMapBuilder;
    final Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMapping;
    final IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping;
    private final AllocationTracker tracker;
    private final IdMappingAllocator.PropertyAllocator propertyAllocator;

    public NodeImporter(
        InternalIdMappingBuilder<? extends IdMappingAllocator> idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMapping,
        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        boolean loadsProperties,
        AllocationTracker tracker
    ) {
        this.idMapBuilder = idMapBuilder;
        this.nodeLabelBitSetMapping = nodeLabelBitSetMapping;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.tracker = tracker;
        this.propertyAllocator = loadsProperties
            ? NodeImporter::importProperties
            : IdMappingAllocator.PropertyAllocator.EMPTY;
    }

    public long importNodes(
        NodesBatchBuffer buffer,
        KernelTransaction kernelTransaction,
        @Nullable NativeNodePropertyImporter propertyImporter
    ) {
        return importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> {
            if (propertyImporter != null) {
                return propertyImporter.importProperties(
                    internalId,
                    nodeReference,
                    labelIds,
                    propertiesReference,
                    kernelTransaction
                );
            } else {
                return 0;
            }
        });
    }

    public long importNodes(NodesBatchBuffer buffer, PropertyReader reader) {
        int batchLength = buffer.length();
        if (batchLength == 0) {
            return 0;
        }

        @Nullable IdMappingAllocator adder = idMapBuilder.allocate(batchLength);
        if (adder == null) {
            return 0;
        }

        int importedProperties = 0;

        var batch = buffer.batch();
        var properties = buffer.properties();
        var labelIds = buffer.labelIds();

        if (buffer.hasLabelInformation()) {
            setNodeLabelInformation(batchLength, adder.startId(), labelIds);
        }

        importedProperties += adder.insert(batch, batchLength, propertyAllocator, reader, properties, labelIds);
        return RawValues.combineIntInt(batchLength, importedProperties);
    }

    private void setNodeLabelInformation(int batchLength, long startIndex, long[][] labelIds) {
        int cappedBatchLength = Math.min(labelIds.length, batchLength);
        for (int i = 0; i < cappedBatchLength; i++) {
            long[] labelIdsForNode = labelIds[i];
            for (long labelId : labelIdsForNode) {
                List<NodeLabel> elementIdentifiers = labelTokenNodeLabelMapping.getOrDefault(
                    (int) labelId,
                    Collections.emptyList()
                );
                for (NodeLabel elementIdentifier : elementIdentifiers) {
                    nodeLabelBitSetMapping
                        .computeIfAbsent(
                            elementIdentifier,
                            (ignored) -> HugeAtomicBitSet.create(idMapBuilder.capacity(), tracker)
                        )
                        .set(startIndex + i);
                }
            }
        }
    }

    private static int importProperties(
        NodeImporter.PropertyReader reader,
        long[] batch,
        long[] properties,
        long[][] labelIds,
        int batchIndex,
        int length,
        long internalIndex
    ) {
        int batchImportedProperties = 0;
        for (int i = 0; i < length; i++) {
            long localIndex = internalIndex + i;
            int indexInBatch = batchIndex + i;
            batchImportedProperties += reader.readProperty(
                batch[indexInBatch],
                labelIds[i],
                properties[indexInBatch],
                localIndex
            );
        }
        return batchImportedProperties;
    }
}
