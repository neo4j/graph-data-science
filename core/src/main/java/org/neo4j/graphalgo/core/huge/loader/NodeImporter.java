/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.huge.loader;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
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

    interface WeightReader {
        int readWeight(long nodeReference, long propertiesReference, long internalId);
    }

    private final HugeLongArrayBuilder idMapBuilder;
    private final IntObjectMap<NodePropertiesBuilder> buildersByPropertyId;
    private final Collection<NodePropertiesBuilder> nodePropertyBuilders;

    public NodeImporter(HugeLongArrayBuilder idMapBuilder, Collection<NodePropertiesBuilder> nodePropertyBuilders) {
        this.idMapBuilder = idMapBuilder;
        this.buildersByPropertyId = mapBuildersByPropertyId(nodePropertyBuilders);
        this.nodePropertyBuilders = nodePropertyBuilders;
    }

    boolean readsProperties() {
        return buildersByPropertyId != null;
    }

    long importNodes(NodesBatchBuffer buffer, Read read, CursorFactory cursors) {
        return importNodes(buffer, (nodeReference, propertiesReference, internalId) ->
                readWeight(nodeReference, propertiesReference, buildersByPropertyId, internalId, cursors, read));
    }

    long importCypherNodes(NodesBatchBuffer buffer, List<Map<String, Number>> cypherNodeProperties) {
        return importNodes(buffer, (nodeReference, propertiesReference, internalId) ->
                readCypherWeight(propertiesReference, internalId, cypherNodeProperties));
    }

    public long importNodes(NodesBatchBuffer buffer, WeightReader reader) {
        int batchLength = buffer.length();
        if (batchLength == 0) {
            return 0;
        }

        HugeLongArrayBuilder.BulkAdder<long[]> adder = idMapBuilder.allocate((long) (batchLength));
        if (adder == null) {
            return 0;
        }

        int importedProperties = 0;

        long[] batch = buffer.batch();
        long[] properties = buffer.properties();
        int batchOffset = 0;
        while (adder.nextBuffer()) {
            int length = adder.length;
            System.arraycopy(batch, batchOffset, adder.buffer, adder.offset, length);

            if (properties != null) {
                long start = adder.start;
                for (int i = 0; i < length; i++) {
                    long localIndex = start + i;
                    int batchIndex = batchOffset + i;
                    importedProperties += reader.readWeight(
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

    private int readWeight(
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
                    double weight = ReadHelper.extractValue(value, defaultValue);
                    props.set(internalId, weight);
                    nodePropertiesRead++;
                }
            }
            return nodePropertiesRead;
        }
    }

    private int readCypherWeight(
            long propertiesReference,
            long internalId,
            List<Map<String, Number>> cypherNodeProperties) {
        Map<String, Number> weights = cypherNodeProperties.get((int) propertiesReference);
        int nodePropertiesRead = 0;
        for (NodePropertiesBuilder props : nodePropertyBuilders) {
            Number number = weights.get(props.propertyKey());
            if (number != null) {
                props.set(internalId, number.doubleValue());
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
