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
import com.carrotsearch.hppc.IntObjectMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, IdsAndProperties> {

    private final GraphCreateFromStoreConfig graphCreateConfig;
    private final ProgressLogger progressLogger;
    private final TerminationFlag terminationFlag;
    private final Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel;

    @Nullable
    private NativeNodePropertyImporter nodePropertyImporter;
    private HugeLongArrayBuilder idMapBuilder;
    private Map<NodeLabel, BitSet> nodeLabelBitSetMapping;

    ScanningNodesImporter(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressLogger progressLogger,
        int concurrency,
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel
    ) {
        super(
            USE_KERNEL_CURSORS ? NodeCursorBasedScanner.FACTORY : NodeRecordBasedScanner.FACTORY,
            "Node",
            loadingContext,
            dimensions,
            concurrency
        );
        this.graphCreateConfig = graphCreateConfig;
        this.progressLogger = progressLogger;
        this.terminationFlag = loadingContext.terminationFlag();
        this.propertyMappingsByNodeLabel = propertyMappingsByNodeLabel;
    }

    @Override
    InternalImporter.CreateScanner creator(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<NodeReference> scanner
    ) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);

        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping = dimensions.tokenNodeLabelMapping();

        nodeLabelBitSetMapping = graphCreateConfig.nodeProjections().allProjections().size() == 1 && labelTokenNodeLabelMapping.containsKey(ANY_LABEL)
            ? Collections.emptyMap()
            : initializeLabelBitSets(nodeCount, labelTokenNodeLabelMapping);

        nodePropertyImporter = initializeNodePropertyImporter(nodeCount);

        return NodesScanner.of(
            api,
            scanner,
            dimensions.nodeLabelTokens(),
            progressLogger,
            new NodeImporter(
                idMapBuilder,
                nodeLabelBitSetMapping,
                labelTokenNodeLabelMapping
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
    private Map<NodeLabel, BitSet> initializeLabelBitSets(
        long nodeCount,
        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping
    ) {
        return StreamSupport.stream(
            labelTokenNodeLabelMapping.values().spliterator(),
            false
        )
            .flatMap(cursor -> cursor.value.stream())
            .distinct()
            .collect(Collectors.toMap(identifier -> identifier, s -> new BitSet(nodeCount)));
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
