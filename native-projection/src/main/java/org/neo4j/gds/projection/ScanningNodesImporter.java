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
package org.neo4j.gds.projection;

import org.immutables.builder.Builder;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.NodeImporterBuilder;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, Nodes> {

    private final LoadablePropertyMappings propertyMappings;
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
        Concurrency concurrency
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

        var propertyMappings = LoadablePropertyMappings.propertyMappings(graphProjectConfig);

        var loadablePropertyMappings = LoadablePropertyMappings.of(graphProjectConfig);

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
        Concurrency concurrency,
        Map<NodeLabel, PropertyMappings> propertyMappingsByLabel,
        LoadablePropertyMappings propertyMappings,
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

        long nodeCount = dimensions.nodeCount();
        int[] labelIds = tokenNodeLabelMapping.keys().toArray();
        return NodeScannerFactory.create(transaction, nodeCount, labelIds, log);
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

        return Nodes.of(
            idMap,
            this.propertyMappingsByLabel,
            nodeProperties,
            PropertyState.PERSISTENT
        );
    }

    private static @Nullable NativeNodePropertyImporter initializeNodePropertyImporter(
        LoadablePropertyMappings propertyMappings,
        GraphDimensions dimensions,
        Concurrency concurrency
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
