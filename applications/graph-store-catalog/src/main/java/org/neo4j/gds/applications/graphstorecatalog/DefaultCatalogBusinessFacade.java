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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.legacycypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.graphstorecatalog.SamplerCompanion.CNARW_CONFIG_PROVIDER;
import static org.neo4j.gds.applications.graphstorecatalog.SamplerCompanion.CNARW_PROVIDER;
import static org.neo4j.gds.applications.graphstorecatalog.SamplerCompanion.RWR_CONFIG_PROVIDER;
import static org.neo4j.gds.applications.graphstorecatalog.SamplerCompanion.RWR_PROVIDER;

/**
 * This layer is shared between Neo4j and other integrations. It is entry-point agnostic.
 * "Business facade" to distinguish it from "procedure facade" and similar.
 * <p>
 * Here we have just business logic: no Neo4j bits or other integration bits, just Java POJO things.
 * <p>
 * By nature business logic is going to be bespoke, so one method per logical thing.
 * Take {@link DefaultCatalogBusinessFacade#graphExists(User, DatabaseId, String)} for example:
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
public class DefaultCatalogBusinessFacade implements CatalogBusinessFacade {
    private final Log log;

    // services
    private final CatalogConfigurationService catalogConfigurationService;
    private final GraphNameValidationService graphNameValidationService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphStoreValidationService graphStoreValidationService;

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

    public DefaultCatalogBusinessFacade(
        Log log,
        CatalogConfigurationService catalogConfigurationService,
        GraphNameValidationService graphNameValidationService,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphStoreValidationService graphStoreValidationService,
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
        WriteNodePropertiesApplication writeNodePropertiesApplication,
        WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication,
        WriteNodeLabelApplication writeNodeLabelApplication,
        WriteRelationshipsApplication writeRelationshipsApplication
    ) {
        this.log = log;

        this.catalogConfigurationService = catalogConfigurationService;
        this.graphNameValidationService = graphNameValidationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.graphStoreValidationService = graphStoreValidationService;

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
    }

    @Override
    public boolean graphExists(User user, DatabaseId databaseId, String graphNameAsString) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        return graphStoreCatalogService.graphExists(user, databaseId, graphName);
    }

    /**
     * @param failIfMissing enable validation that graphs exist before dropping them
     * @param databaseName  optional override
     * @param username      optional override
     * @throws IllegalArgumentException if a database name was null or blank or not a String
     */
    @Override
    public List<GraphStoreWithConfig> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username,
        DatabaseId currentDatabase,
        User operator
    ) {
        // general parameter consolidation
        // we imagine any new endpoints will follow the exact same parameter lists I guess, for now
        var validatedGraphNames = parseGraphNameOrListOfGraphNames(graphNameOrListOfGraphNames);
        var databaseId = currentDatabase.orOverride(databaseName);
        var usernameOverride = User.parseUsernameOverride(username);

        return dropGraphApplication.compute(validatedGraphNames, failIfMissing, databaseId, operator, usernameOverride);
    }

    @Override
    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        var validatedGraphName = graphNameValidationService.validatePossibleNull(graphName);

        return listGraphApplication.list(user, validatedGraphName, includeDegreeDistribution, terminationFlag);
    }

    @Override
    public GraphProjectNativeResult nativeProject(
        User user,
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(user, databaseId, graphNameAsString);

        var configuration = catalogConfigurationService.parseNativeProjectConfiguration(
            user,
            graphName,
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        );

        return nativeProjectApplication.project(
            databaseId,
            graphDatabaseService,
            graphProjectMemoryUsageService,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    @Override
    public MemoryEstimateResult estimateNativeProject(
        DatabaseId databaseId,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
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
            databaseId,
            graphProjectMemoryUsageService,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    @Override
    public GraphProjectCypherResult cypherProject(
        User user,
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(user, databaseId, graphNameAsString);

        var configuration = catalogConfigurationService.parseCypherProjectConfiguration(
            user,
            graphName,
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        );

        return cypherProjectApplication.project(
            databaseId,
            graphDatabaseService,
            graphProjectMemoryUsageService,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    @Override
    public MemoryEstimateResult estimateCypherProject(
        DatabaseId databaseId,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
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
            databaseId,
            graphProjectMemoryUsageService,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    @Override
    public GraphFilterResult subGraphProject(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String originGraphNameAsString,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(user, databaseId, graphNameAsString);
        var originGraphName = graphNameValidationService.validate(originGraphNameAsString);

        graphStoreCatalogService.ensureGraphExists(user, databaseId, originGraphName);

        var originGraphConfiguration = graphStoreCatalogService.get(
            CatalogRequest.of(user.getUsername(), databaseId),
            originGraphName
        );

        var configuration = catalogConfigurationService.parseSubGraphProjectConfiguration(
            user,
            graphName,
            originGraphName,
            nodeFilter,
            relationshipFilter,
            originGraphConfiguration,
            rawConfiguration
        );

        return subGraphProjectApplication.project(
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration,
            originGraphConfiguration.graphStore()
        );
    }

    @Override
    public GraphMemoryUsage sizeOf(User user, DatabaseId databaseId, String graphNameAsString) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        if (!graphStoreCatalogService.graphExists(user, databaseId, graphName)) {
            throw new IllegalArgumentException("Graph '" + graphNameAsString + "' does not exist");
        }

        return graphMemoryUsageApplication.sizeOf(
            user,
            databaseId,
            graphName
        );
    }

    @Override
    public GraphDropNodePropertiesResult dropNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
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
        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureNodePropertiesExist(graphStore, configuration.nodeProperties());

        var numberOfPropertiesRemoved = dropNodePropertiesApplication.compute(
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration,
            graphStore
        );

        return new GraphDropNodePropertiesResult(
            graphName.getValue(),
            configuration.nodeProperties(),
            numberOfPropertiesRemoved
        );
    }

    @Override
    public GraphDropRelationshipResult dropRelationships(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String relationshipType
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipsMayBeDeleted(graphStore, relationshipType, graphName);

        var result = dropRelationshipsApplication.compute(
            taskRegistryFactory,
            userLogRegistryFactory,
            graphStore,
            relationshipType
        );

        return new GraphDropRelationshipResult(graphName.getValue(), relationshipType, result);
    }

    @Override
    public long dropGraphProperty(
        User user,
        DatabaseId databaseId,
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

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
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
        User user,
        DatabaseId databaseId,
        String graphNameAsString,
        String nodeLabel,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
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
        User user,
        DatabaseId databaseId,
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

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureGraphPropertyExists(graphStore, graphProperty);

        return graphStore.graphPropertyValues(graphProperty).objects();
    }

    @Override
    public <T> Stream<T> streamNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
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
        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
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
            taskRegistryFactory,
            userLogRegistryFactory,
            graphStore,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );
    }

    @Override
    public <T> Stream<T> streamRelationshipProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
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

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipPropertiesMatchRelationshipTypes(graphStore, configuration);

        return streamRelationshipPropertiesApplication.compute(
            taskRegistryFactory,
            userLogRegistryFactory,
            graphStore,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );
    }

    @Override
    public Stream<TopologyResult> streamRelationships(
        User user,
        DatabaseId databaseId,
        String graphNameAsString,
        List<String> relationshipTypes,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = catalogConfigurationService.parseGraphStreamRelationshipsConfiguration(
            graphName,
            relationshipTypes,
            rawConfiguration
        );

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipTypesPresent(
            graphStore,
            configuration.relationshipTypeIdentifiers(graphStore)
        );

        return streamRelationshipsApplication.compute(graphStore, configuration);
    }

    @Override
    public NodePropertiesWriteResult writeNodeProperties(
        User user,
        DatabaseId databaseId,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
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

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
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
            graphStore,
            nodePropertyExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            graphName,
            configuration
        );
    }

    @Override
    public WriteRelationshipPropertiesResult writeRelationshipProperties(
        User user,
        DatabaseId databaseId,
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        TerminationFlag terminationFlag,
        String graphNameAsString,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        // why graphstore first here?
        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensureRelationshipPropertiesMatchRelationshipType(
            graphStore,
            relationshipType,
            relationshipProperties
        );

        // maybe because this configuration is non-functionals only?
        var configuration = catalogConfigurationService.parseWriteRelationshipPropertiesConfiguration(rawConfiguration);

        return writeRelationshipPropertiesApplication.compute(
            relationshipPropertiesExporterBuilder,
            terminationFlag,
            graphStore,
            graphName,
            relationshipType,
            relationshipProperties,
            configuration
        );
    }

    @Override
    public WriteLabelResult writeNodeLabel(
        User user,
        DatabaseId databaseId,
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        TerminationFlag terminationFlag,
        String graphNameAsString,
        String nodeLabel,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = graphNameValidationService.validate(graphNameAsString);

        var configuration = WriteLabelConfig.of(rawConfiguration);

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();

        var nodeFilter = NodeFilterParser.parseAndValidate(graphStore, configuration.nodeFilter());

        return writeNodeLabelApplication.compute(
            nodeLabelExporterBuilder,
            terminationFlag,
            graphStore,
            graphName,
            nodeLabel,
            configuration,
            nodeFilter
        );
    }

    @Override
    public WriteRelationshipResult writeRelationships(
        User user,
        DatabaseId databaseId,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
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

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), graphName);
        var graphStore = graphStoreWithConfig.graphStore();
        graphStoreValidationService.ensurePossibleRelationshipPropertyMatchesRelationshipType(
            graphStore,
            configuration.relationshipType(),
            configuration.relationshipProperty()
        );

        return writeRelationshipsApplication.compute(
            relationshipExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            graphStore,
            graphName,
            configuration
        );
    }

    @Override
    public RandomWalkSamplingResult sampleRandomWalkWithRestarts(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        return sampleRandomWalk(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            originGraphName,
            configuration,
            RWR_CONFIG_PROVIDER,
            RWR_PROVIDER
        );
    }

    @Override
    public RandomWalkSamplingResult sampleCommonNeighbourAwareRandomWalk(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        return sampleRandomWalk(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphNameAsString,
            originGraphName,
            configuration,
            CNARW_CONFIG_PROVIDER,
            CNARW_PROVIDER
        );
    }

    @Override
    public MemoryEstimateResult estimateCommonNeighbourAwareRandomWalk(
        User user,
        DatabaseId databaseId,
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = catalogConfigurationService.parseCommonNeighbourAwareRandomWalkConfig(rawConfiguration);

        return estimateCommonNeighbourAwareRandomWalkApplication.estimate(user, databaseId, graphName, configuration);
    }

    @Override
    public GraphGenerationStats generateGraph(
        User user,
        DatabaseId databaseId,
        String graphNameAsString,
        long nodeCount,
        long averageDegree,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = ensureGraphNameValidAndUnknown(user, databaseId, graphNameAsString);

        var configuration = catalogConfigurationService.parseRandomGraphGeneratorConfig(
            user,
            graphName,
            nodeCount,
            averageDegree,
            rawConfiguration
        );

        return generateGraphApplication.compute(databaseId, averageDegree, configuration);
    }

    private RandomWalkSamplingResult sampleRandomWalk(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String originGraphNameAsString,
        Map<String, Object> configuration,
        Function<CypherMapWrapper, RandomWalkWithRestartsConfig> samplerConfigProvider,
        Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> samplerAlgorithmProvider
    ) {
        var graphName = ensureGraphNameValidAndUnknown(user, databaseId, graphNameAsString);
        var originGraphName = GraphName.parse(originGraphNameAsString);

        var graphStoreWithConfig = graphStoreCatalogService.get(CatalogRequest.of(user, databaseId), originGraphName);
        var graphStore = graphStoreWithConfig.graphStore();
        var graphProjectConfig = graphStoreWithConfig.config();

        return graphSamplingApplication.sample(
            user,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphStore,
            graphProjectConfig,
            originGraphName,
            graphName,
            configuration,
            samplerConfigProvider,
            samplerAlgorithmProvider
        );
    }

    private GraphName ensureGraphNameValidAndUnknown(User user, DatabaseId databaseId, String graphNameAsString) {
        var graphName = graphNameValidationService.validateStrictly(graphNameAsString);

        graphStoreCatalogService.ensureGraphDoesNotExist(user, databaseId, graphName);

        return graphName;
    }

    /**
     * I wonder if other endpoint will also deliver an Object type to parse in this layer - we shall see
     */
    private List<GraphName> parseGraphNameOrListOfGraphNames(Object graphNameOrListOfGraphNames) {
        return graphNameValidationService.validateSingleOrList(graphNameOrListOfGraphNames);
    }
}
