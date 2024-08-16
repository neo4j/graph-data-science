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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;

/**
 * This exists so that we may supply only _some_ applications when creating the facade. Handy for testing.
 */
class DefaultGraphCatalogApplicationsBuilder {
    // global dependencies
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final ProjectionMetricsService projectionMetricsService;
    private final GraphNameValidationService graphNameValidationService;

    // applications
    private DropGraphApplication dropGraphApplication;
    private ListGraphApplication listGraphApplication;
    private NativeProjectApplication nativeProjectApplication;
    private CypherProjectApplication cypherProjectApplication;
    private SubGraphProjectApplication subGraphProjectApplication;
    private GraphMemoryUsageApplication graphMemoryUsageApplication;
    private DropNodePropertiesApplication dropNodePropertiesApplication;
    private DropRelationshipsApplication dropRelationshipsApplication;
    private NodeLabelMutatorApplication nodeLabelMutatorApplication;
    private StreamNodePropertiesApplication streamNodePropertiesApplication;
    private StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication;
    private StreamRelationshipsApplication streamRelationshipsApplication;
    private WriteNodePropertiesApplication writeNodePropertiesApplication;
    private WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication;
    private WriteNodeLabelApplication writeNodeLabelApplication;
    private WriteRelationshipsApplication writeRelationshipsApplication;
    private GraphSamplingApplication graphSamplingApplication;
    private EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication;
    private GenerateGraphApplication generateGraphApplication;
    private ExportToCsvApplication exportToCsvApplication;
    private ExportToCsvEstimateApplication exportToCsvEstimateApplication;
    private ExportToDatabaseApplication exportToDatabaseApplication;

    public DefaultGraphCatalogApplicationsBuilder(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ProjectionMetricsService projectionMetricsService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this(
            log,
            graphStoreCatalogService,
            projectionMetricsService,
            requestScopedDependencies,
            new GraphNameValidationService()
        );
    }

    /**
     * @param graphNameValidationService _Sometimes_ you want to inject the name validation service, e.g. for testing
     */
    DefaultGraphCatalogApplicationsBuilder(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ProjectionMetricsService projectionMetricsService,
        RequestScopedDependencies requestScopedDependencies,
        GraphNameValidationService graphNameValidationService
    ) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.projectionMetricsService = projectionMetricsService;
        this.graphNameValidationService = graphNameValidationService;
    }

    DefaultGraphCatalogApplicationsBuilder withCypherProjectApplication(CypherProjectApplication cypherProjectApplication) {
        this.cypherProjectApplication = cypherProjectApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withDropGraphApplication(DropGraphApplication dropGraphApplication) {
        this.dropGraphApplication = dropGraphApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withDropNodePropertiesApplication(DropNodePropertiesApplication dropNodePropertiesApplication) {
        this.dropNodePropertiesApplication = dropNodePropertiesApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withDropRelationshipsApplication(DropRelationshipsApplication dropRelationshipsApplication) {
        this.dropRelationshipsApplication = dropRelationshipsApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withEstimateCommonNeighbourAwareRandomWalkApplication(
        EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication
    ) {
        this.estimateCommonNeighbourAwareRandomWalkApplication = estimateCommonNeighbourAwareRandomWalkApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withExportToCsvApplication(ExportToCsvApplication exportToCsvApplication) {
        this.exportToCsvApplication = exportToCsvApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withExportToCsvEstimateApplication(ExportToCsvEstimateApplication exportToCsvEstimateApplication) {
        this.exportToCsvEstimateApplication = exportToCsvEstimateApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withExportToDatabaseApplication(ExportToDatabaseApplication exportToDatabaseApplication) {
        this.exportToDatabaseApplication = exportToDatabaseApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withGenerateGraphApplication(GenerateGraphApplication generateGraphApplication) {
        this.generateGraphApplication = generateGraphApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withGraphMemoryUsageApplication(GraphMemoryUsageApplication graphMemoryUsageApplication) {
        this.graphMemoryUsageApplication = graphMemoryUsageApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withGraphSamplingApplication(GraphSamplingApplication graphSamplingApplication) {
        this.graphSamplingApplication = graphSamplingApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withListGraphApplication(ListGraphApplication listGraphApplication) {
        this.listGraphApplication = listGraphApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withNativeProjectApplication(NativeProjectApplication nativeProjectApplication) {
        this.nativeProjectApplication = nativeProjectApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withNodeLabelMutatorApplication(NodeLabelMutatorApplication nodeLabelMutatorApplication) {
        this.nodeLabelMutatorApplication = nodeLabelMutatorApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withStreamNodePropertiesApplication(StreamNodePropertiesApplication streamNodePropertiesApplication) {
        this.streamNodePropertiesApplication = streamNodePropertiesApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withStreamRelationshipPropertiesApplication(
        StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication
    ) {
        this.streamRelationshipPropertiesApplication = streamRelationshipPropertiesApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withStreamRelationshipsApplication(StreamRelationshipsApplication streamRelationshipsApplication) {
        this.streamRelationshipsApplication = streamRelationshipsApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withSubGraphProjectApplication(SubGraphProjectApplication subGraphProjectApplication) {
        this.subGraphProjectApplication = subGraphProjectApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withWriteNodeLabelApplication(WriteNodeLabelApplication writeNodeLabelApplication) {
        this.writeNodeLabelApplication = writeNodeLabelApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withWriteNodePropertiesApplication(WriteNodePropertiesApplication writeNodePropertiesApplication) {
        this.writeNodePropertiesApplication = writeNodePropertiesApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withWriteRelationshipPropertiesApplication(
        WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication
    ) {
        this.writeRelationshipPropertiesApplication = writeRelationshipPropertiesApplication;
        return this;
    }

    DefaultGraphCatalogApplicationsBuilder withWriteRelationshipsApplication(WriteRelationshipsApplication writeRelationshipsApplication) {
        this.writeRelationshipsApplication = writeRelationshipsApplication;
        return this;
    }

    DefaultGraphCatalogApplications build() {
        return new DefaultGraphCatalogApplications(
            log,
            graphStoreCatalogService,
            projectionMetricsService,
            graphNameValidationService,
            requestScopedDependencies,
            cypherProjectApplication,
            dropGraphApplication,
            dropNodePropertiesApplication,
            dropRelationshipsApplication,
            estimateCommonNeighbourAwareRandomWalkApplication,
            generateGraphApplication,
            graphMemoryUsageApplication,
            graphSamplingApplication,
            listGraphApplication,
            nativeProjectApplication,
            nodeLabelMutatorApplication,
            streamNodePropertiesApplication,
            streamRelationshipPropertiesApplication,
            streamRelationshipsApplication,
            subGraphProjectApplication,
            writeNodeLabelApplication,
            writeNodePropertiesApplication,
            writeRelationshipPropertiesApplication,
            writeRelationshipsApplication,
            exportToCsvApplication,
            exportToCsvEstimateApplication,
            exportToDatabaseApplication
        );
    }
}
