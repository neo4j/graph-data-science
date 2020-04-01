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

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongObjectMap;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;

public class NodeImporter {

    interface PropertyReader {
        int readProperty(long nodeReference, long[] labelIds, long propertiesReference, long internalId);
    }

    final Map<ElementIdentifier, BitSet> elementIdentifierBitSetMapping;
    final LongObjectMap<List<ElementIdentifier>> labelElementIdentifierMapping;

    private final HugeLongArrayBuilder idMapBuilder;

    public NodeImporter(HugeLongArrayBuilder idMapBuilder) {
        this(idMapBuilder, null, null);
    }

    public NodeImporter(
        HugeLongArrayBuilder idMapBuilder,
        Map<ElementIdentifier, BitSet> elementIdentifierBitSetMapping,
        LongObjectMap<List<ElementIdentifier>> labelElementIdentifierMapping
    ) {
        this.idMapBuilder = idMapBuilder;
        this.elementIdentifierBitSetMapping = elementIdentifierBitSetMapping;
        this.labelElementIdentifierMapping = labelElementIdentifierMapping;
    }

    long importNodes(NodesBatchBuffer buffer, Read read, CursorFactory cursors, NativeNodePropertyImporter propertyImporter) {
        return importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> {
            if (propertyImporter != null) {
                return propertyImporter.importProperties(internalId, nodeReference, labelIds, propertiesReference, cursors, read);
            } else {
                return 0;
            }
        });
    }

    long importCypherNodes(NodesBatchBuffer buffer, List<Map<String, Number>> cypherNodeProperties, CypherNodePropertyImporter propertyImporter) {
        return importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> {
            if (propertyImporter != null) {
                return propertyImporter.importProperties(internalId, labelIds, (int) propertiesReference, cypherNodeProperties);
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

        HugeLongArrayBuilder.BulkAdder<long[]> adder = idMapBuilder.allocate(batchLength);
        if (adder == null) {
            return 0;
        }

        int importedProperties = 0;

        long[] batch = buffer.batch();
        long[] properties = buffer.properties();

        if (buffer.hasLabelInformation()) {
            setNodeLabelInformation(batchLength, adder.start, buffer.labelIds());
        }

        int batchOffset = 0;
        while (adder.nextBuffer()) {
            int length = adder.length;
            System.arraycopy(batch, batchOffset, adder.buffer, adder.offset, length);

            if (properties != null) {
                long start = adder.start;
                for (int i = 0; i < length; i++) {
                    long localIndex = start + i;
                    int batchIndex = batchOffset + i;
                    importedProperties += reader.readProperty(
                        batch[batchIndex],
                        buffer.labelIds()[i],
                        properties[batchIndex],
                        localIndex
                    );
                }
            }
            batchOffset += length;
        }
        return RawValues.combineIntInt(batchLength, importedProperties);
    }

    private void setNodeLabelInformation(int batchLength, long startIndex, long[][] labelIds) {
        int cappedBatchLength = Math.min(labelIds.length, batchLength);
        for (int i = 0; i < cappedBatchLength; i++) {
            long[] labelIdsForNode = labelIds[i];
            for (long labelId : labelIdsForNode) {
                List<ElementIdentifier> elementIdentifiers = labelElementIdentifierMapping.getOrDefault(labelId, Collections.emptyList());
                for (ElementIdentifier elementIdentifier : elementIdentifiers) {
                    elementIdentifierBitSetMapping
                        .computeIfAbsent(elementIdentifier, (ignore) -> new BitSet(batchLength))
                        .set(startIndex + i);
                }
            }
        }

        // set the whole range for '*' projections
        for (ElementIdentifier allProjectionIdentifier : labelElementIdentifierMapping.getOrDefault(ANY_LABEL, Collections.emptyList())) {
            elementIdentifierBitSetMapping.get(allProjectionIdentifier).set(startIndex, startIndex + batchLength);
        }
    }
}
