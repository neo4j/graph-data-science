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
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeRecord, IdsAndProperties> {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;
    private final Map<ElementIdentifier, PropertyMappings> propertyMappings;

    private NativeNodePropertyImporter nodePropertyImporter;
    private HugeLongArrayBuilder idMapBuilder;
    private Map<ElementIdentifier, BitSet> elementIdentifierBitSetMapping;

    ScanningNodesImporter(
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ProgressLogger progressLogger,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            ExecutorService threadPool,
            int concurrency,
            Map<ElementIdentifier, PropertyMappings> propertyMappings
    ) {
        super(NodeStoreScanner.NODE_ACCESS, "Node", api, dimensions, threadPool, concurrency);
        this.progressLogger = progressLogger;
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


        LongObjectMap<List<ElementIdentifier>> labelIdentifierMapping = dimensions.labelElementIdentifierMapping();

        Supplier<Map<ElementIdentifier, BitSet>> initializeLabelBitSets = () ->
            StreamSupport.stream(
                labelIdentifierMapping.values().spliterator(),
                false
            )
                .flatMap(cursor -> cursor.value.stream())
                .distinct()
                .collect(Collectors.toMap(identifier -> identifier, s -> new BitSet(nodeCount)));


        elementIdentifierBitSetMapping = labelIdentifierMapping.size() == 1 && labelIdentifierMapping.containsKey(ANY_LABEL)
            ? null
            : initializeLabelBitSets.get();

        nodePropertyImporter = NativeNodePropertyImporter
            .builder()
            .nodeCount(nodeCount)
            .concurrency(concurrency)
            .dimensions(dimensions)
            .propertyMappings(propertyMappings)
            .tracker(tracker)
            .build();

        return NodesScanner.of(
            api,
            scanner,
            dimensions.nodeLabelIds(),
            progressLogger,
            new NodeImporter(
                idMapBuilder,
                elementIdentifierBitSetMapping,
                labelIdentifierMapping
            ),
            nodePropertyImporter,
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

        return IdsAndProperties.of(hugeIdMap, nodePropertyImporter.result());
    }
}
