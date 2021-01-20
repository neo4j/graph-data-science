/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import com.carrotsearch.hppc.LongObjectMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeRecord, IdsAndProperties> {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;
    private final Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel;

    @Nullable
    private NativeNodePropertyImporter nodePropertyImporter;
    private HugeLongArrayBuilder idMapBuilder;
    private Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMapping;

    ScanningNodesImporter(
        GraphDatabaseAPI api,
        GraphDimensions dimensions,
        ProgressLogger progressLogger,
        AllocationTracker tracker,
        TerminationFlag terminationFlag,
        ExecutorService threadPool,
        int concurrency,
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel
    ) {
        super(NodeStoreScanner.NODE_ACCESS, "Node", api, dimensions, threadPool, concurrency);
        this.progressLogger = progressLogger;
        this.tracker = tracker;
        this.terminationFlag = terminationFlag;
        this.propertyMappingsByNodeLabel = propertyMappingsByNodeLabel;
    }

    @Override
    InternalImporter.CreateScanner creator(
        long nodeCount,
        ImportSizing sizing,
        AbstractStorePageCacheScanner<NodeRecord> scanner
    ) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);

        LongObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping = dimensions.labelTokenNodeLabelMapping();

        nodeLabelBitSetMapping = labelTokenNodeLabelMapping.size() == 1 && labelTokenNodeLabelMapping.containsKey(ANY_LABEL)
            ? null
            : initializeLabelBitSets(nodeCount, labelTokenNodeLabelMapping);

        nodePropertyImporter = initializeNodePropertyImporter(nodeCount);

        return NodesScanner.of(
            api,
            scanner,
            dimensions.nodeLabelIds(),
            progressLogger,
            new NodeImporter(
                idMapBuilder,
                nodeLabelBitSetMapping,
                labelTokenNodeLabelMapping,
                tracker
            ),
            nodePropertyImporter,
            terminationFlag
        );
    }

    @Override
    IdsAndProperties build() {
        IdMap hugeIdMap = IdMapBuilder.build(
            idMapBuilder,
            nodeLabelBitSetMapping,
            dimensions.highestNeoId(),
            concurrency,
            tracker
        );

        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties = nodePropertyImporter == null
            ? new HashMap<>()
            : nodePropertyImporter.result();

        return IdsAndProperties.of(hugeIdMap, nodeProperties);
    }

    @NotNull
    private Map<NodeLabel, HugeAtomicBitSet> initializeLabelBitSets(
        long nodeCount,
        LongObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping
    ) {
        return StreamSupport.stream(
            labelTokenNodeLabelMapping.values().spliterator(),
            false
        )
            .flatMap(cursor -> cursor.value.stream())
            .distinct()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> HugeAtomicBitSet.create(nodeCount, tracker)
                )
            );
    }

    @Nullable
    private NativeNodePropertyImporter initializeNodePropertyImporter(long nodeCount) {
        boolean loadProperties = propertyMappingsByNodeLabel
            .values()
            .stream()
            .anyMatch(mappings -> mappings.numberOfMappings() > 0);

        if (loadProperties) {
            return NativeNodePropertyImporter
                .builder()
                .nodeCount(nodeCount)
                .concurrency(concurrency)
                .dimensions(dimensions)
                .propertyMappings(propertyMappingsByNodeLabel)
                .tracker(tracker)
                .build();
        } else {
            return null;
        }
    }
}
