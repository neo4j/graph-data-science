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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.beta.filter.GraphFilterResult;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.io.GraphStoreExporterBaseConfig;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.logging.GdsLoggers;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.graphsampling.RandomWalkSamplerType;
import org.neo4j.gds.legacycypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.graphsampling.RandomWalkSamplerType.CNARW;
import static org.neo4j.gds.graphsampling.RandomWalkSamplerType.RWR;

/**
 * This layer is shared between Neo4j and other integrations. It is entry-point agnostic.
 * "Business facade" to distinguish it from "procedure facade" and similar.
 * <p>
 * Here we have just business logic: no Neo4j bits or other integration bits, just Java POJO things.
 * <p>
 * By nature business logic is going to be bespoke, so one method per logical thing.
 * Take {@link GraphCatalogApplications#graphExists(org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies, String)} for example:
 * pure expressed business logic that layers above will use in multiple places, but!
 * Any marshalling happens in those layers, not here.
 * <p>
 * General validations could go here, think "graph exists" or "graph name not blank".
 * Also, this is where you would put cross-cutting concerns, things that many pieces of business logic share.
 * Generally though, a facade is really handy for others to pull in as a single dependency,
 * not for hosting all teh codez. _Maybe_ you stick your business logic in here directly,
 * if it is just one line or two; let's not be religious.
 * Ideally though this is a facade over many individual pieces of business logic in separate classes,
 * or behind other facades (oh gosh turtles, turtles everywhere :scream:).
 */
public class DefaultGraphCatalogApplications implements GraphCatalogApplications {
    private final CatalogConfigurationService catalogConfigurationService = new CatalogConfigurationService();
    private final GraphStoreValidationService graphStoreValidationService = new GraphStoreValidationService();

    // global dependencies
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final ProjectionMetricsService projectionMetricsService;

    // services
    private final GraphNameValidationService graphNameValidationService;

    // applications
    private final DropGraphApplication dropGraphApplication;
    private final ListGraphApplication listGraphApplication;
    private final NativeProjectApplication nativeProjectApplication;
    private final CypherProjectApplication cypherProjectApplication;
    private final SubGraphProjectApplication subGraphProjectApplication;
    private final GraphMemoryUsageApplication graphMemoryUsageApplication;
    private final DropNodePropertiesApplication dropNodePropertiesApplication;
    private final DropRelationshipsApplication dropRelationshipsApplication;
    private final NodeLabelMutatorApplication nodeLabelMutatorApplication;
    private final StreamNodePropertiesApplication streamNodePropertiesApplication;
    private final StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication;
    private final StreamRelationshipsApplication streamRelationshipsApplication;
    private final WriteNodePropertiesApplication writeNodePropertiesApplication;
    private final WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication;
    private final WriteNodeLabelApplication writeNodeLabelApplication;
    private final WriteRelationshipsApplication writeRelationshipsApplication;
    private final GraphSamplingApplication graphSamplingApplication;
    private final EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication;
    private final GenerateGraphApplication generateGraphApplication;
    private final ExportToCsvApplication exportToCsvApplication;
    private final ExportToCsvEstimateApplication exportToCsvEstimateApplication;
    private final ExportToDatabaseApplication exportToDatabaseApplication;

    DefaultGraphCatalogApplications(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ProjectionMetricsService projectionMetricsService,
        GraphNameValidationService graphNameValidationService,
        CypherProjectApplication cypherProjectApplication,
        DropGraphApplication dropGraphApplication,
        DropNodePropertiesApplication dropNodePropertiesApplication,
        DropRelationshipsApplication dropRelationshipsApplication,
        EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication,
        GenerateGraphApplication generateGraphApplication,
        GraphMemoryUsageApplication graphMemoryUsageApplication,
        GraphSamplingApplication graphSamplingApplication,
        ListGraphApplication listGraphApplication,
        NativeProjectApplication nativeProjectApplication,
        NodeLabelMutatorApplication nodeLabelMutatorApplication,
        StreamNodePropertiesApplication streamNodePropertiesApplication,
        StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication,
        StreamRelationshipsApplication streamRelationshipsApplication,
        SubGraphProjectApplication subGraphProjectApplication,
        WriteNodeLabelApplication writeNodeLabelApplication,
        WriteNodePropertiesApplication writeNodePropertiesApplication,
        WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication,
        WriteRelationshipsApplication writeRelationshipsApplication,
        ExportToCsvApplication exportToCsvApplication,
        ExportToCsvEstimateApplication exportToCsvEstimateApplication,
        ExportToDatabaseApplication exportToDatabaseApplication
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.projectionMetricsService = projectionMetricsService;

        this.graphNameValidationService = graphNameValidationService;

        this.dropGraphApplication = dropGraphApplication;
        this.listGraphApplication = listGraphApplication;
        this.nativeProjectApplication = nativeProjectApplication;
        this.cypherProjectApplication = cypherProjectApplication;
        this.subGraphProjectApplication = subGraphProjectApplication;
        this.graphMemoryUsageApplication = graphMemoryUsageApplication;
        this.dropNodePropertiesApplication = dropNodePropertiesApplication;
        this.dropRelationshipsApplication = dropRelationshipsApplication;
        this.nodeLabelMutatorApplication = nodeLabelMutatorApplication;
        this.streamNodePropertiesApplication = streamNodePropertiesApplication;
        this.streamRelationshipPropertiesApplication = streamRelationshipPropertiesApplication;
        this.streamRelationshipsApplication = streamRelationshipsApplication;
        this.writeNodePropertiesApplication = writeNodePropertiesApplication;
        this.writeRelationshipPropertiesApplication = writeRelationshipPropertiesApplication;
        this.writeNodeLabelApplication = writeNodeLabelApplication;
        this.writeRelationshipsApplication = writeRelationshipsApplication;
        this.graphSamplingApplication = graphSamplingApplication;
        this.estimateCommonNeighbourAwareRandomWalkApplication = estimateCommonNeighbourAwareRandomWalkApplication;
        this.generateGraphApplication = generateGraphApplication;
        this.exportToCsvApplication = exportToCsvApplication;
        this.exportToCsvEstimateApplication = exportToCsvEstimateApplication;
        this.exportToDatabaseApplication = exportToDatabaseApplication;
    }

    public static GraphCatalogApplications create(
        GdsLoggers loggers,
        ExportLocation exportLocation,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        ProjectionMetricsService projectionMetricsService,
        GraphDatabaseService graphDatabaseService,
        Transaction procedureTransaction
    ) {
        var graphNameValidationService = new GraphNameValidationService();

        var cypherProjectApplication = new CypherProjectApplication(
            new GenericProjectApplication<>(
                loggers.log(),
                graphStoreCatalogService,
                graphStoreFactorySuppliers,
                GraphProjectCypherResult.Builder::new
            )
        );
        var dropGraphApplication = new DropGraphApplication(graphStoreCatalogService);
        var dropNodePropertiesApplication = new DropNodePropertiesApplication(loggers.loggerForProgressTracking());
        var dropRelationshipsApplication = new DropRelationshipsApplication(loggers.loggerForProgressTracking());
        var estimateCommonNeighbourAwareRandomWalkApplication = new EstimateCommonNeighbourAwareRandomWalkApplication();
        var exportToCsvApplication = new ExportToCsvApplication(
            loggers,
            graphDatabaseService,
            procedureTransaction,
            exportLocation
        );
        var exportToCsvEstimateApplication = new ExportToCsvEstimateApplication();
        var exportToDatabaseApplication = new ExportToDatabaseApplication(
            loggers,
            graphDatabaseService,
            procedureTransaction
        );
        var generateGraphApplication = new GenerateGraphApplication(loggers.log(), graphStoreCatalogService);
        var graphMemoryUsageApplication = new GraphMemoryUsageApplication(graphStoreCatalogService);
        var graphSamplingApplication = new GraphSamplingApplication(
            loggers.loggerForProgressTracking(),
            graphStoreCatalogService
        );
        var listGraphApplication = ListGraphApplication.create(graphStoreCatalogService);
        var nativeProjectApplication = new NativeProjectApplication(
            new GenericProjectApplication<>(
                loggers.log(),
                graphStoreCatalogService,
                graphStoreFactorySuppliers,
                GraphProjectNativeResult.Builder::new
            )
        );
        var nodeLabelMutatorApplication = new NodeLabelMutatorApplication();
        var streamNodePropertiesApplication = new StreamNodePropertiesApplication(loggers.loggerForProgressTracking());
        var streamRelationshipPropertiesApplication = new StreamRelationshipPropertiesApplication(loggers.loggerForProgressTracking());
        var streamRelationshipsApplication = new StreamRelationshipsApplication();
        var subGraphProjectApplication = new SubGraphProjectApplication(
            loggers,
            graphStoreCatalogService
        );
        var writeNodeLabelApplication = new WriteNodeLabelApplication(loggers.log());
        var writeNodePropertiesApplication = new WriteNodePropertiesApplication(loggers);
        var writeRelationshipPropertiesApplication = new WriteRelationshipPropertiesApplication(loggers.log());
        var writeRelationshipsApplication = new WriteRelationshipsApplication(loggers);

        return new DefaultGraphCatalogApplicationsBuilder(
            loggers.log(),
            graphStoreCatalogService,
            projectionMetricsService,
            graphNameValidationService
        )
            .withCypherProjectApplication(cypherProjectApplication)
            .withDropGraphApplication(dropGraphApplication)
            .withDropNodePropertiesApplication(dropNodePropertiesApplication)
            .withDropRelationshipsApplication(dropRelationshipsApplication)
            .withEstimateCommonNeighbourAwareRandomWalkApplication(estimateCommonNeighbourAwareRandomWalkApplication)
            .withExportToCsvApplication(exportToCsvApplication)
            .withExportToCsvEstimateApplication(exportToCsvEstimateApplication)
            .withExportToDatabaseApplication(exportToDatabaseApplication)
            .withGenerateGraphApplication(generateGraphApplication)
            .withGraphMemoryUsageApplication(graphMemoryUsageApplication)
            .withGraphSamplingApplication(graphSamplingApplication)
            .withListGraphApplication(listGraphApplication)
            .withNativeProjectApplication(nativeProjectApplication)
            .withNodeLabelMutatorApplication(nodeLabelMutatorApplication)
            .withStreamNodePropertiesApplication(streamNodePropertiesApplication)
            .withStreamRelationshipPropertiesApplication(streamRelationshipPropertiesApplication)
            .withStreamRelationshipsApplication(streamRelationshipsApplication)
            .withSubGraphProjectApplication(subGraphProjectApplication)
            .withWriteNodeLabelApplication(writeNodeLabelApplication)
            .withWriteNodePropertiesApplication(writeNodePropertiesApplication)
            .withWriteRelationshipPropertiesApplication(writeRelationshipPropertiesApplication)
            .withWriteRelationshipsApplication(writeRelationshipsApplication)
            .build();
    }

    @Override
    public boolean graphExists(RequestScopedDependencies requestScopedDependencies, String graphNameAsString) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        return graphStoreCatalogService.graphExists(
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId(),
            graphName
        );
    }

    /**
     * @param failIfMissing             enable validation that graphs exist before dropping them
     * @param databaseNameOverride      optional override
     * @param usernameOverride          optional override
     * @throws IllegalArgumentException if a database name was null or blank or not a String
     */
    @Override
    public List<GraphStoreCatalogEntry> dropGraph(
        RequestScopedDependencies requestScopedDependencies,
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseNameOverride,
        String usernameOverride
    ) {
        // general parameter consolidation
        // we imagine any new endpoints will follow the exact same parameter lists I guess, for now
        var validatedGraphNames = parseGraphNameOrListOfGraphNames(graphNameOrListOfGraphNames);
        var databaseId = requestScopedDependencies.databaseId().orOverride(databaseNameOverride);
        var parsedUsernameOverride = User.parseUsernameOverride(usernameOverride);

        return dropGraphApplication.compute(
            validatedGraphNames,
            failIfMissing,
            databaseId,
            requestScopedDependencies.user(),
            parsedUsernameOverride
        );
    }

    @Override
    public List<Pair<GraphStoreCatalogEntry, Map<String, Object>>> listGraphs(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        boolean includeDegreeDistribution
    ) {
        var validatedGraphName = graphNameValidationService.validatePossibleNull(graphName);

        return listGraphApplication.list(
            requestScopedDependencies,
            validatedGraphName,
            includeDegreeDistribution
        );
    }

    @Override
    public GraphProjectNativeResult nativeProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String graphNameAsString,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(requestScopedDependencies, graphNameAsString);

        var configuration = catalogConfigurationService.parseNativeProjectConfiguration(
            requestScopedDependencies.user(),
            graphName,
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        );

        var projectMetric = projectionMetricsService.createNative();
        try (projectMetric) {
            projectMetric.start();
            return nativeProjectApplication.project(
                requestScopedDependencies,
                graphDatabaseService,
                graphProjectMemoryUsageService,
                transactionContext,
                configuration
            );
        } catch (Exception e) {
            projectMetric.failed(e);
            throw e;
        }
    }

    @Override
    public MemoryEstimateResult estimateNativeProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = catalogConfigurationService.parseEstimateNativeProjectConfiguration(
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        );

        return nativeProjectApplication.estimate(
            requestScopedDependencies,
            graphProjectMemoryUsageService,
            transactionContext,
            configuration
        );
    }

    @Override
    public GraphProjectCypherResult cypherProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String graphNameAsString,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(requestScopedDependencies, graphNameAsString);

        var configuration = catalogConfigurationService.parseCypherProjectConfiguration(
            requestScopedDependencies.user(),
            graphName,
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        );

        var projectMetric = projectionMetricsService.createCypher();
        try (projectMetric) {
            projectMetric.start();
            return cypherProjectApplication.project(
                requestScopedDependencies,
                graphDatabaseService,
                graphProjectMemoryUsageService,
                transactionContext,
                configuration
            );
        } catch (Exception e) {
            projectMetric.failed(e);
            throw e;
        }
    }

    @Override
    public MemoryEstimateResult estimateCypherProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = catalogConfigurationService.parseEstimateCypherProjectConfiguration(
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        );

        return cypherProjectApplication.estimate(
            requestScopedDependencies,
            graphProjectMemoryUsageService,
            transactionContext,
            configuration
        );
    }

    @Override
    public GraphFilterResult subGraphProject(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String originGraphNameAsString,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(requestScopedDependencies, graphNameAsString);
        var originGraphName = graphNameValidationService.validate(originGraphNameAsString);

        graphStoreCatalogService.ensureGraphExists(
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId(),
            originGraphName
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, originGraphName);

        var configuration = catalogConfigurationService.parseSubGraphProjectConfiguration(
            requestScopedDependencies.user(),
            graphName,
            originGraphName,
            nodeFilter,
            relationshipFilter,
            graphStoreWithConfig,
            rawConfiguration
        );

        var subGraphMetric = projectionMetricsService.createSubGraph();
        try (subGraphMetric) {
            subGraphMetric.start();
            return subGraphProjectApplication.project(
                requestScopedDependencies,
                configuration,
                graphStoreWithConfig.graphStore()
            );
        } catch (Exception e) {
            subGraphMetric.failed(e);
            throw e;
        }
    }

    @Override
    public GraphMemoryUsage sizeOf(RequestScopedDependencies requestScopedDependencies, String graphNameAsString) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        if (!graphStoreCatalogService.graphExists(
            requestScopedDependencies.user(),
            requestScopedDependencies.databaseId(),
            graphName
        )) {
            throw new IllegalArgumentException("Graph '" + graphNameAsString + "' does not exist");
        }

        return graphMemoryUsageApplication.sizeOf(
            requestScopedDependencies,
            graphName
        );
    }

    @Override
    public GraphDropNodePropertiesResult dropNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Object nodeProperties,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphDropNodePropertiesConfiguration(
            graphName,
            nodeProperties,
            rawConfiguration
        );

        // melt this together, so you only obtain the graph store if it is valid? think it over
        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();

        var droppedProperties = gatherDroppedProperties(
            graphStore,
            configuration.nodeProperties(),
            configuration.failIfMissing()
        );

        var numberOfPropertiesRemoved = dropNodePropertiesApplication.compute(
            requestScopedDependencies,
            droppedProperties,
            graphStore
        );

        return new GraphDropNodePropertiesResult(
            graphName.value(),
            droppedProperties,
            numberOfPropertiesRemoved
        );
    }

    private List<String> gatherDroppedProperties(
        GraphStore graphStore,
        List<String> nodeProperties,
        boolean failIfMissing
    ) {
        if (failIfMissing) {
            graphStoreValidationService.ensureNodePropertiesExist(graphStore, nodeProperties);
            return nodeProperties;
        } else {
            return graphStoreValidationService.filterExistingNodeProperties(graphStore, nodeProperties);
        }
    }

    @Override
    public GraphDropRelationshipResult dropRelationships(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String relationshipType
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipsMayBeDeleted(graphStore, relationshipType, graphName);

        var result = dropRelationshipsApplication.compute(
            requestScopedDependencies,
            graphStore,
            relationshipType
        );

        return new GraphDropRelationshipResult(
            graphName.value(),
            relationshipType, result.deletedRelationships(), result.deletedProperties());
    }

    @Override
    public long dropGraphProperty(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String graphProperty,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        // we do this for the side effect of checking for unknown configuration keys, it is a bit naff
        catalogConfigurationService.validateDropGraphPropertiesConfiguration(
            graphName,
            graphProperty,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureGraphPropertyExists(graphStore, graphProperty);

        var numberOfProperties = graphStore.graphPropertyValues(graphProperty).valueCount();

        try {
            graphStore.removeGraphProperty(graphProperty);
        } catch (RuntimeException e) {
            log.warn("Graph property removal failed", e);
            throw e;
        }

        return numberOfProperties;
    }

    @Override
    public MutateLabelResult mutateNodeLabel(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String nodeLabel,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();

        // stick in configuration service returning a pair?
        var configuration = MutateLabelConfig.of(rawConfiguration);
        var nodeFilter = NodeFilterParser.parseAndValidate(graphStore, configuration.nodeFilter());

        return nodeLabelMutatorApplication.compute(
            graphStore,
            graphName,
            nodeLabel,
            configuration,
            nodeFilter
        );
    }

    @Override
    public Stream<?> streamGraphProperty(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String graphProperty,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        catalogConfigurationService.validateGraphStreamGraphPropertiesConfig(
            graphName,
            graphProperty,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureGraphPropertyExists(graphStore, graphProperty);

        return graphStore.graphPropertyValues(graphProperty).objects();
    }

    @Override
    public <T> Stream<T> streamNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Object nodePropertiesAsObject,
        Object nodeLabelsAsObject,
        Map<String, Object> rawConfiguration,
        boolean usesPropertyNameColumn,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStreamNodePropertiesConfiguration(
            graphName,
            nodePropertiesAsObject,
            nodeLabelsAsObject,
            rawConfiguration
        );

        // melt this together, so you only obtain the graph store if it is valid? think it over
        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var nodeLabels = configuration.nodeLabels();
        var nodeLabelIdentifiers = configuration.nodeLabelIdentifiers(graphStore);
        var nodeProperties = configuration.nodeProperties();
        graphStoreValidationService.ensureNodePropertiesMatchNodeLabels(
            graphStore,
            nodeLabels,
            nodeLabelIdentifiers,
            nodeProperties
        );

        return streamNodePropertiesApplication.compute(
            requestScopedDependencies,
            graphStore,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );
    }

    @Override
    public <T> Stream<T> streamRelationshipProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        List<String> relationshipProperties,
        Object relationshipTypes,
        Map<String, Object> rawConfiguration,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStreamRelationshipPropertiesConfiguration(
            graphName,
            relationshipProperties,
            relationshipTypes,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipPropertiesMatchRelationshipTypes(graphStore, configuration);

        return streamRelationshipPropertiesApplication.compute(
            requestScopedDependencies,
            graphStore,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );
    }

    @Override
    public Stream<TopologyResult> streamRelationships(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Object relationshipTypes,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStreamRelationshipsConfiguration(
            graphName,
            relationshipTypes,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipTypesPresent(
            graphStore,
            configuration.relationshipTypeIdentifiers(graphStore)
        );

        return streamRelationshipsApplication.compute(graphStore, configuration);
    }

    @Override
    public NodePropertiesWriteResult writeNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        String graphNameAsString,
        Object nodePropertiesAsObject,
        Object nodeLabelsAsObject,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphWriteNodePropertiesConfiguration(
            graphName,
            nodePropertiesAsObject,
            nodeLabelsAsObject,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var resultStore = graphStoreWithConfig.resultStore();
        var nodeLabels = configuration.nodeLabels();
        var nodeLabelIdentifiers = configuration.nodeLabelIdentifiers(graphStore);
        var nodeProperties = configuration.nodeProperties().stream()
            .map(UserInputWriteProperties.PropertySpec::nodeProperty)
            .collect(Collectors.toList());

        graphStoreValidationService.ensureNodePropertiesMatchNodeLabels(
            graphStore,
            nodeLabels,
            nodeLabelIdentifiers,
            nodeProperties
        );

        return writeNodePropertiesApplication.write(
            requestScopedDependencies,
            graphStore,
            resultStore,
            nodePropertyExporterBuilder,
            graphName,
            configuration
        );
    }

    @Override
    public WriteRelationshipPropertiesResult writeRelationshipProperties(
        RequestScopedDependencies requestScopedDependencies,
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        String graphNameAsString,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        // why graphstore first here?
        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var resultStore = graphStoreWithConfig.resultStore();
        graphStoreValidationService.ensureRelationshipPropertiesMatchRelationshipType(
            graphStore,
            relationshipType,
            relationshipProperties
        );

        // maybe because this configuration is non-functionals only?
        var configuration = catalogConfigurationService.parseWriteRelationshipPropertiesConfiguration(rawConfiguration);

        return writeRelationshipPropertiesApplication.compute(
            relationshipPropertiesExporterBuilder,
            requestScopedDependencies.terminationFlag(),
            graphStore,
            resultStore,
            graphName,
            relationshipType,
            relationshipProperties,
            configuration
        );
    }

    @Override
    public WriteLabelResult writeNodeLabel(
        RequestScopedDependencies requestScopedDependencies,
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        String graphNameAsString,
        String nodeLabel,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = WriteLabelConfig.of(rawConfiguration);

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var resultStore = graphStoreWithConfig.resultStore();

        var nodeFilter = NodeFilterParser.parseAndValidate(graphStore, configuration.nodeFilter());

        return writeNodeLabelApplication.compute(
            nodeLabelExporterBuilder,
            requestScopedDependencies.terminationFlag(),
            graphStore,
            resultStore,
            graphName,
            nodeLabel,
            configuration,
            nodeFilter
        );
    }

    @Override
    public WriteRelationshipResult writeRelationships(
        RequestScopedDependencies requestScopedDependencies,
        RelationshipExporterBuilder relationshipExporterBuilder,
        String graphNameAsString,
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphWriteRelationshipConfiguration(
            relationshipType,
            relationshipProperty,
            rawConfiguration
        );

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var resultStore = graphStoreWithConfig.resultStore();

        graphStoreValidationService.ensurePossibleRelationshipPropertyMatchesRelationshipType(
            graphStore,
            configuration.relationshipType(),
            configuration.relationshipProperty()
        );

        return writeRelationshipsApplication.compute(
            requestScopedDependencies,
            relationshipExporterBuilder,
            graphStore,
            resultStore,
            graphName,
            configuration
        );
    }

    @Override
    public RandomWalkSamplingResult sampleRandomWalkWithRestarts(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        return sampleRandomWalk(
            requestScopedDependencies,
            graphName,
            originGraphName,
            configuration,
            RWR
        );
    }

    @Override
    public RandomWalkSamplingResult sampleCommonNeighbourAwareRandomWalk(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        return sampleRandomWalk(
            requestScopedDependencies,
            graphNameAsString,
            originGraphName,
            configuration,
            CNARW
        );
    }

    @Override
    public MemoryEstimateResult estimateCommonNeighbourAwareRandomWalk(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = catalogConfigurationService.parseCommonNeighbourAwareRandomWalkConfig(rawConfiguration);

        return estimateCommonNeighbourAwareRandomWalkApplication.estimate(requestScopedDependencies, graphName, configuration);
    }

    @Override
    public GraphGenerationStats generateGraph(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        long nodeCount,
        long averageDegree,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(requestScopedDependencies, graphNameAsString);

        var configuration = catalogConfigurationService.parseRandomGraphGeneratorConfig(
            requestScopedDependencies.user(),
            graphName,
            nodeCount,
            averageDegree,
            rawConfiguration
        );

        return generateGraphApplication.compute(requestScopedDependencies.databaseId(), averageDegree, configuration);
    }

    @Override
    public FileExportResult exportToCsv(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStoreToFileExporterConfiguration(
            requestScopedDependencies.user(),
            rawConfiguration
        );

        var graphStore = getGraphStoreAndValidateForExport(requestScopedDependencies, graphName, configuration);

        return exportToCsvApplication.run(requestScopedDependencies, graphName, configuration, graphStore);
    }

    @Override
    public MemoryEstimateResult exportToCsvEstimate(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Map<String, Object> rawConfiguration) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStoreToCsvEstimationConfiguration(
            requestScopedDependencies.user(),
            rawConfiguration
        );

        var graphStore = getGraphStore(requestScopedDependencies, graphName, configuration);

        return exportToCsvEstimateApplication.run(configuration, graphStore);
    }

    @Override
    public DatabaseExportResult exportToDatabase(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStoreToDatabaseExporterConfig(rawConfiguration);

        var graphStore = getGraphStoreAndValidateForExport(requestScopedDependencies, graphName, configuration);

        return exportToDatabaseApplication.run(requestScopedDependencies, graphName, configuration, graphStore);
    }

    private GraphStore getGraphStoreAndValidateForExport(
        RequestScopedDependencies requestScopedDependencies,
        GraphName graphName,
        GraphStoreExporterBaseConfig configuration
    ) {
        var graphStore = getGraphStore(requestScopedDependencies, graphName, configuration);

        var shouldExportAdditionalNodeProperties = !configuration.additionalNodeProperties().mappings().isEmpty();

        graphStoreValidationService.ensureReadAccess(graphStore, shouldExportAdditionalNodeProperties);
        graphStoreValidationService.ensureNodePropertiesNotExist(graphStore, configuration.additionalNodeProperties());

        return graphStore;
    }

    private GraphStore getGraphStore(RequestScopedDependencies requestScopedDependencies, GraphName graphName, BaseConfig configuration) {
        var graphStoreCatalogEntry = graphStoreCatalogService.getGraphStoreCatalogEntry(
            graphName,
            requestScopedDependencies.user(),
            configuration.usernameOverride(),
            requestScopedDependencies.databaseId()
        );

        return graphStoreCatalogEntry.graphStore();
    }

    private RandomWalkSamplingResult sampleRandomWalk(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String originGraphNameAsString,
        Map<String, Object> configuration,
        RandomWalkSamplerType samplerType
    ) {
        var graphName = ensureGraphNameValidAndUnknown(requestScopedDependencies, graphNameAsString);
        var originGraphName = GraphName.parse(originGraphNameAsString);

        var graphStoreWithConfig = getGraphStoreWithConfig(requestScopedDependencies, originGraphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var graphProjectConfig = graphStoreWithConfig.config();

        var samplingMetric = projectionMetricsService.createRandomWakSampling(samplerType.name());
        try (samplingMetric) {
            samplingMetric.start();
            return graphSamplingApplication.sample(
                requestScopedDependencies,
                graphStore,
                graphProjectConfig,
                originGraphName,
                graphName,
                configuration,
                samplerType
            );
        } catch (Exception e) {
            samplingMetric.failed(e);
            throw e;
        }

    }

    private GraphName ensureGraphNameValidAndUnknown(RequestScopedDependencies requestScopedDependencies, String graphNameAsString) {
        var graphName = graphNameValidationService.validateStrictly(graphNameAsString);

        graphStoreCatalogService.ensureGraphDoesNotExist(requestScopedDependencies.user(), requestScopedDependencies.databaseId(), graphName);

        return graphName;
    }

    /**
     * I wonder if other endpoint will also deliver an Object type to parse in this layer - we shall see
     */
    private List<GraphName> parseGraphNameOrListOfGraphNames(Object graphNameOrListOfGraphNames) {
        return graphNameValidationService.validateSingleOrList(graphNameOrListOfGraphNames);
    }

    private GraphStoreCatalogEntry getGraphStoreWithConfig(
        RequestScopedDependencies requestScopedDependencies,
        GraphName graphName
    ) {
        return graphStoreCatalogService.get(
            CatalogRequest.of(
                requestScopedDependencies.user(),
                requestScopedDependencies.databaseId()
            ), graphName
        );
    }
}
