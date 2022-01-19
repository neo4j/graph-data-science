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
package org.neo4j.gds.core.loading;

import org.immutables.builder.Builder;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.logging.Log;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, IdsAndProperties> {

    private final IndexPropertyMappings.LoadablePropertyMappings propertyMappings;
    private final TerminationFlag terminationFlag;
    private final InternalIdMappingBuilder idMapBuilder;
    private final LabelInformation.Builder labelInformationBuilder;
    private final @Nullable NativeNodePropertyImporter nodePropertyImporter;

    @Builder.Factory
    public static ScanningNodesImporter scanningNodesImporter(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        int concurrency,
        IndexPropertyMappings.LoadablePropertyMappings propertyMappings,
        InternalIdMappingBuilder idMapBuilder
    ) {
        var allocationTracker = loadingContext.allocationTracker();
        var expectedCapacity = dimensions.highestPossibleNodeCount();
        var labelTokenNodeLabelMapping = dimensions.tokenNodeLabelMapping();

        var scannerFactory = scannerFactory(loadingContext.transactionContext(), dimensions, loadingContext.log());

        var labelInformationBuilder =
            graphProjectConfig.nodeProjections().allProjections().size() == 1
            && labelTokenNodeLabelMapping.containsKey(ANY_LABEL)
                ? LabelInformation.emptyBuilder(allocationTracker)
                : LabelInformation.builder(expectedCapacity, labelTokenNodeLabelMapping, allocationTracker);

        var nodePropertyImporter = initializeNodePropertyImporter(
            propertyMappings,
            dimensions,
            concurrency,
            allocationTracker
        );

        return new ScanningNodesImporter(
            scannerFactory,
            loadingContext,
            dimensions,
            progressTracker,
            concurrency,
            propertyMappings,
            nodePropertyImporter,
            idMapBuilder,
            labelInformationBuilder
        );
    }

    private ScanningNodesImporter(
        StoreScanner.Factory<NodeReference> scannerFactory,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        int concurrency,
        IndexPropertyMappings.LoadablePropertyMappings propertyMappings,
        @Nullable NativeNodePropertyImporter nodePropertyImporter,
        InternalIdMappingBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder
    ) {
        super(
            scannerFactory,
            loadingContext,
            dimensions,
            progressTracker,
            concurrency
        );

        this.terminationFlag = loadingContext.terminationFlag();
        this.propertyMappings = propertyMappings;
        this.nodePropertyImporter = nodePropertyImporter;
        this.idMapBuilder = idMapBuilder;
        this.labelInformationBuilder = labelInformationBuilder;
    }

    private static StoreScanner.Factory<NodeReference> scannerFactory(
        TransactionContext transaction,
        GraphDimensions dimensions,
        Log log
    ) {
        var tokenNodeLabelMapping = dimensions.tokenNodeLabelMapping();
        assert tokenNodeLabelMapping != null : "Only null in Cypher loader";

        int[] labelIds = tokenNodeLabelMapping.keys().toArray();
        return NodeScannerFactory.create(transaction, labelIds, log);
    }

    @Override
    public RecordScannerTaskRunner.RecordScannerTaskFactory recordScannerTaskFactory(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<NodeReference> storeScanner
    ) {
        var nodeImporter = new NodeImporter(
            idMapBuilder,
            labelInformationBuilder,
            dimensions.tokenNodeLabelMapping(),
            nodePropertyImporter != null
        );

        return NodesScannerTask.factory(
            transaction,
            storeScanner,
            dimensions.highestPossibleNodeCount(),
            dimensions.nodeLabelTokens(),
            progressTracker,
            nodeImporter,
            nodePropertyImporter,
            terminationFlag
        );
    }

    @Override
    public IdsAndProperties build() {
        var nodeMapping = idMapBuilder.build(
            labelInformationBuilder,
            Math.max(dimensions.highestPossibleNodeCount() - 1, 0),
            concurrency,
            false,
            allocationTracker
        );

        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties = nodePropertyImporter == null
            ? new HashMap<>()
            : nodePropertyImporter.result(nodeMapping);

        if (!propertyMappings.indexedProperties().isEmpty()) {
            importPropertiesFromIndex(nodeMapping, nodeProperties);
        }

        return IdsAndProperties.of(nodeMapping, nodeProperties);
    }

    private void importPropertiesFromIndex(
        NodeMapping nodeMapping,
        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties
    ) {
        long indexStart = System.nanoTime();

        try {
            progressTracker.beginSubTask("Property Index Scan");

            var parallelIndexScan = GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled();
            // In order to avoid a race between preparing the importers and the flag being toggled,
            // we set the concurrency to 1 if we don't import in parallel.
            //
            // !! NOTE !!
            //   If you end up changing this logic
            //   you need to add a check for the feature flag to IndexedNodePropertyImporter
            var concurrency = parallelIndexScan ? this.concurrency : 1;

            var indexScanningImporters = propertyMappings.indexedProperties()
                .entrySet()
                .stream()
                .flatMap(labelAndProperties -> labelAndProperties
                    .getValue()
                    .mappings()
                    .stream()
                    .map(mappingAndIndex -> new IndexedNodePropertyImporter(
                        concurrency,
                        transaction,
                        labelAndProperties.getKey(),
                        mappingAndIndex.property(),
                        mappingAndIndex.index(),
                        nodeMapping,
                        progressTracker,
                        terminationFlag,
                        executorService,
                        allocationTracker
                    ))
                ).collect(Collectors.toList());

            if (!parallelIndexScan) {
                // While we don't scan the index in parallel, try to at least scan all properties in parallel
                ParallelUtil.run(indexScanningImporters, executorService);
            }
            long recordsImported = 0L;
            for (IndexedNodePropertyImporter propertyImporter : indexScanningImporters) {
                if (parallelIndexScan) {
                    // If we run in parallel, we need to run the importers one after another, as they will
                    // parallelize internally
                    propertyImporter.run();
                }
                var nodeLabel = propertyImporter.nodeLabel();
                var storeProperties = nodeProperties.computeIfAbsent(nodeLabel, ignore -> new HashMap<>());
                storeProperties.put(propertyImporter.mapping(), propertyImporter.build(nodeMapping));
                recordsImported += propertyImporter.imported();
            }

            long tookNanos = System.nanoTime() - indexStart;
            BigInteger bigNanos = BigInteger.valueOf(tookNanos);
            double tookInSeconds = new BigDecimal(bigNanos)
                .divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING)
                .doubleValue();
            double recordsPerSecond = new BigDecimal(A_BILLION)
                .multiply(BigDecimal.valueOf(recordsImported))
                .divide(new BigDecimal(bigNanos), 9, RoundingMode.CEILING)
                .doubleValue();

            progressTracker.logDebug(formatWithLocale(
                "Property Index Scan: Imported %,d properties; took %.3f s, %,.2f Properties/s",
                recordsImported,
                tookInSeconds,
                recordsPerSecond
            ));
        } finally {
            progressTracker.endSubTask("Property Index Scan");
        }
    }

    private static @Nullable NativeNodePropertyImporter initializeNodePropertyImporter(
        IndexPropertyMappings.LoadablePropertyMappings propertyMappings,
        GraphDimensions dimensions,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        var propertyMappingsByLabel = propertyMappings.storedProperties();
        boolean loadProperties = propertyMappingsByLabel
            .values()
            .stream()
            .anyMatch(mappings -> mappings.numberOfMappings() > 0);

        if (loadProperties) {
            return NativeNodePropertyImporter
                .builder()
                .concurrency(concurrency)
                .dimensions(dimensions)
                .propertyMappings(propertyMappingsByLabel)
                .allocationTracker(allocationTracker)
                .build();
        }

        return null;
    }
}
