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
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.logging.Log;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, Nodes> {

    private final IndexPropertyMappings.LoadablePropertyMappings propertyMappings;
    private final Map<NodeLabel, PropertyMappings> propertyMappingsByLabel;
    private final TerminationFlag terminationFlag;
    private final IdMapBuilder idMapBuilder;
    private final LabelInformation.Builder labelInformationBuilder;
    private final @Nullable NativeNodePropertyImporter nodePropertyImporter;

    @Builder.Factory
    public static ScanningNodesImporter scanningNodesImporter(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker,
        int concurrency
    ) {
        var expectedCapacity = dimensions.highestPossibleNodeCount();

        var scannerFactory = scannerFactory(loadingContext.transactionContext(), dimensions, loadingContext.log());

        var idMapBuilder = IdMapBehaviorServiceProvider
            .idMapBehavior()
            .create(
                concurrency,
                Optional.of(dimensions.highestPossibleNodeCount()),
                Optional.of(dimensions.nodeCount())
            );

        LabelInformation.Builder labelInformationBuilder;
        if (graphProjectConfig.nodeProjections().allProjections().size() == 1) {
            var singleLabel = graphProjectConfig.nodeProjections().projections().keySet().iterator().next();
            labelInformationBuilder = LabelInformationBuilders.singleLabel(singleLabel);
        } else {
            labelInformationBuilder = LabelInformationBuilders.multiLabelWithCapacityAndLabelInformation(
                expectedCapacity,
                dimensions.availableNodeLabels(),
                dimensions.starNodeLabelMappings()
            );
        }

        var propertyMappings = IndexPropertyMappings.propertyMappings(graphProjectConfig);

        var loadablePropertyMappings = IndexPropertyMappings.prepareProperties(
            graphProjectConfig,
            dimensions,
            loadingContext.transactionContext()
        );

        var nodePropertyImporter = initializeNodePropertyImporter(
            loadablePropertyMappings,
            dimensions,
            concurrency
        );

        return new ScanningNodesImporter(
            scannerFactory,
            loadingContext,
            dimensions,
            progressTracker,
            concurrency,
            propertyMappings,
            loadablePropertyMappings,
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
        Map<NodeLabel, PropertyMappings> propertyMappingsByLabel,
        IndexPropertyMappings.LoadablePropertyMappings propertyMappings,
        @Nullable NativeNodePropertyImporter nodePropertyImporter,
        IdMapBuilder idMapBuilder,
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
        this.propertyMappingsByLabel = propertyMappingsByLabel;
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
        var nodeImporter = new NodeImporterBuilder()
            .idMapBuilder(idMapBuilder)
            .labelInformationBuilder(labelInformationBuilder)
            .labelTokenNodeLabelMapping(dimensions.tokenNodeLabelMapping())
            .importProperties(nodePropertyImporter != null)
            .build();

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
    public Nodes build() {
        var idMap = idMapBuilder.build(
            labelInformationBuilder,
            Math.max(dimensions.highestPossibleNodeCount() - 1, 0),
            concurrency
        );

        Map<PropertyMapping, NodePropertyValues> nodeProperties = nodePropertyImporter == null
            ? new HashMap<>()
            : nodePropertyImporter.result(idMap);

        if (!propertyMappings.indexedProperties().isEmpty()) {
            importPropertiesFromIndex(idMap, nodeProperties);
        }

        return Nodes.of(idMap, this.propertyMappingsByLabel, nodeProperties, PropertyState.PERSISTENT);
    }

    private void importPropertiesFromIndex(
        IdMap idMap,
        Map<PropertyMapping, NodePropertyValues> nodeProperties
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

            var buildersByPropertyKey = new HashMap<PropertyMapping, NodePropertiesFromStoreBuilder>();
            propertyMappings.indexedProperties()
                .values()
                .stream()
                .flatMap(propertyMappings -> propertyMappings.mappings().stream())
                .forEach(propertyMapping ->
                    buildersByPropertyKey.put(propertyMapping.property(), NodePropertiesFromStoreBuilder.of(propertyMapping.property().defaultValue(), concurrency)));

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
                        idMap,
                        progressTracker,
                        terminationFlag,
                        executorService,
                        buildersByPropertyKey.get(mappingAndIndex.property())
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
            }

            for (var entry : buildersByPropertyKey.entrySet()) {
                NodePropertyValues propertyValues = entry.getValue().build(idMap);
                nodeProperties.put(entry.getKey(), propertyValues);
                recordsImported += propertyValues.valuesStored();
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
        int concurrency
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
                .build();
        }

        return null;
    }
}
