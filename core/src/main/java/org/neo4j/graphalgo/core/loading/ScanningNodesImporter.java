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
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeRecord, IdsAndProperties> {

    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;
    private final PropertyMappings propertyMappings;

    private Map<String, NodePropertiesBuilder> builders;
    private HugeLongArrayBuilder idMapBuilder;
    private Map<ElementIdentifier, BitSet> elementIdentifierBitSetMapping;

    ScanningNodesImporter(
        GraphDatabaseAPI api,
        GraphDimensions dimensions,
        ImportProgress progress,
        AllocationTracker tracker,
        TerminationFlag terminationFlag,
        ExecutorService threadPool,
        int concurrency,
        PropertyMappings propertyMappings
    ) {
        super(NodeStoreScanner.NODE_ACCESS, "Node", api, dimensions, threadPool, concurrency);
        this.progress = progress;
        this.tracker = tracker;
        this.terminationFlag = terminationFlag;
        this.propertyMappings = propertyMappings;
    }

    @Override
    InternalImporter.CreateScanner creator(
        long nodeCount,
        ImportSizing sizing,
        AbstractStorePageCacheScanner<NodeRecord> scanner
    ) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);

        elementIdentifierBitSetMapping = StreamSupport.stream(
            dimensions
                .labelElementIdentifierMapping()
                .values()
                .spliterator(),
            false)
            .flatMap(cursor -> cursor.value.stream())
            .distinct()
            .collect(Collectors.toMap(identifier -> identifier, s -> new BitSet(nodeCount)));

        builders = propertyBuilders(nodeCount);
        return NodesScanner.of(
            api,
            scanner,
            dimensions.nodeLabelIds(),
            progress,
            new NodeImporter(idMapBuilder, elementIdentifierBitSetMapping, builders.values(), dimensions.labelElementIdentifierMapping()),
            terminationFlag
        );
    }

    @Override
    IdsAndProperties build() {
        IdMap hugeIdMap = IdMapBuilder.build(
            idMapBuilder,
            elementIdentifierBitSetMapping,
            dimensions.highestNeoId(),
            concurrency,
            tracker
        );
        Map<String, NodeProperties> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : propertyMappings) {
            NodePropertiesBuilder builder = builders.get(propertyMapping.propertyKey());
            NodeProperties props = builder != null ? builder.build() : new NullPropertyMap(propertyMapping.defaultValue());
            nodeProperties.put(propertyMapping.propertyKey(), props);
        }
        return new IdsAndProperties(hugeIdMap, Collections.unmodifiableMap(nodeProperties));
    }

    private Map<String, NodePropertiesBuilder> propertyBuilders(long nodeCount) {
        Map<String, NodePropertiesBuilder> builders = new HashMap<>();
        for (ResolvedPropertyMapping resolvedPropertyMapping : dimensions.nodeProperties()) {
            if (resolvedPropertyMapping.exists()) {
                NodePropertiesBuilder builder = NodePropertiesBuilder.of(
                    nodeCount,
                    tracker,
                    resolvedPropertyMapping.defaultValue(),
                    resolvedPropertyMapping.propertyKeyId(),
                    resolvedPropertyMapping.propertyKey(),
                    concurrency
                );
                builders.put(resolvedPropertyMapping.propertyKey(), builder);
            }
        }
        return builders;
    }
}
