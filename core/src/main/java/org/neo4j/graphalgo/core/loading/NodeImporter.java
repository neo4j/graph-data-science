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
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.graphalgo.core.utils.BitSetBuilder;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class NodeImporter {

    interface PropertyReader {
        int readProperty(long nodeReference, long propertiesReference, long internalId);
    }

    private final HugeLongArrayBuilder idMapBuilder;
    private final Map<String, BitSetBuilder> labelInformationBuilders;
    private final IntObjectMap<NodePropertiesBuilder> buildersByPropertyId;
    private final Collection<NodePropertiesBuilder> nodePropertyBuilders;

    public NodeImporter(
        HugeLongArrayBuilder idMapBuilder,
        Map<String, BitSetBuilder> labelInformationBuilders,
        Collection<NodePropertiesBuilder> nodePropertyBuilders
    ) {
        this.idMapBuilder = idMapBuilder;
        this.labelInformationBuilders = labelInformationBuilders;
        this.buildersByPropertyId = mapBuildersByPropertyId(nodePropertyBuilders);
        this.nodePropertyBuilders = nodePropertyBuilders;
    }

    boolean readsProperties() {
        return buildersByPropertyId != null;
    }

    long importNodes(NodesBatchBuffer buffer, Read read, CursorFactory cursors) {
        return importNodes(buffer, (nodeReference, propertiesReference, internalId) ->
                readProperty(nodeReference, propertiesReference, buildersByPropertyId, internalId, cursors, read));
    }

    long importCypherNodes(NodesBatchBuffer buffer, List<Map<String, Number>> cypherNodeProperties) {
        return importNodes(buffer, (nodeReference, propertiesReference, internalId) ->
                readCypherProperty(propertiesReference, internalId, cypherNodeProperties));
    }

    public long nodeCount() {
        return idMapBuilder.length();
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
        Map<String, BitSet> labelInformation = buffer.labelBitSets();
        if (labelInformation != null) {
            labelInformation
                .keySet()
                .forEach(elementIdentifier -> labelInformationBuilders.get(elementIdentifier).bulkAdd(labelInformation.get(elementIdentifier)));
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
                            properties[batchIndex],
                            localIndex
                    );
                }
            }
            batchOffset += length;
        }
        return RawValues.combineIntInt(batchLength, importedProperties);
    }

    private int readProperty(
            long nodeReference,
            long propertiesReference,
            IntObjectMap<NodePropertiesBuilder> nodeProperties,
            long internalId,
            CursorFactory cursors,
            Read read) {
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            read.nodeProperties(nodeReference, propertiesReference, pc);
            int nodePropertiesRead = 0;
            while (pc.next()) {
                NodePropertiesBuilder props = nodeProperties.get(pc.propertyKey());
                if (props != null) {
                    Value value = pc.propertyValue();
                    double defaultValue = props.defaultValue();
                    double propertyValue = ReadHelper.extractValue(value, defaultValue);
                    props.set(internalId, propertyValue);
                    nodePropertiesRead++;
                }
            }
            return nodePropertiesRead;
        }
    }

    private int readCypherProperty(
            long propertiesReference,
            long internalId,
            List<Map<String, Number>> cypherNodeProperties) {
        Map<String, Number> properties = cypherNodeProperties.get((int) propertiesReference);
        int nodePropertiesRead = 0;
        for (NodePropertiesBuilder props : nodePropertyBuilders) {
            Number propertyValue = properties.get(props.propertyKey());
            if (propertyValue != null) {
                props.set(internalId, propertyValue.doubleValue());
                nodePropertiesRead++;
            }
        }
        return nodePropertiesRead;
    }


    private IntObjectMap<NodePropertiesBuilder> mapBuildersByPropertyId(Collection<NodePropertiesBuilder> builders) {
        if (builders == null) {
            return null;
        }
        IntObjectMap<NodePropertiesBuilder> map = new IntObjectHashMap<>(builders.size());
        builders.stream().filter(builder -> builder.propertyId() >= 0).forEach(builder -> map.put(builder.propertyId(), builder));
        return map.isEmpty() ? null : map;
    }
}
