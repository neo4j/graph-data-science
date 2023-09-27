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
package org.neo4j.gds.procedures.catalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.GraphGenerationStats;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsage;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertyOrPropertiesResultProducer;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertyResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertyOrPropertiesResultProducer;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertyResult;
import org.neo4j.gds.applications.graphstorecatalog.MutateLabelResult;
import org.neo4j.gds.applications.graphstorecatalog.NodePropertiesWriteResult;
import org.neo4j.gds.applications.graphstorecatalog.RandomWalkSamplingResult;
import org.neo4j.gds.applications.graphstorecatalog.TopologyResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteLabelResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipPropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipResult;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.procedures.ProcedureTransactionService;
import org.neo4j.gds.procedures.TransactionContextService;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Catalog facade groups all catalog procedures in one class, for ease of dependency injection and management.
 * Behaviour captured here is Neo4j procedure specific stuff only,
 * everything else gets pushed down into domain and business logic. Conversely,
 * any actual Neo4j procedure specific behaviour should live here and not in procedure stubs.
 * This allows us to keep the procedure stub classes dumb and thin, and one day generateable.
 * <p>
 * This class gets constructed per request, and as such has fields for request scoped things like user and database id.
 */
public class CatalogFacade {
    /**
     * This exists because procedures need default values sometimes.
     * For example, CALL gds.graph.list() would fail otherwise,
     * the user would have to do something silly like CALL gds.graph.list("")
     */
    public static final String NO_VALUE_PLACEHOLDER = "d9b6394a-9482-4929-adab-f97df578a6c6";

    // services
    private final Consumer<AutoCloseable> streamCloser;
    private final DatabaseId databaseId;
    private final GraphDatabaseService graphDatabaseService;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final ProcedureTransactionService procedureTransactionService;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;
    private final TransactionContextService transactionContextService;
    private final User user;
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final UserLogServices userLogServices;

    // business facade
    private final CatalogBusinessFacade businessFacade;

    /**
     * @param streamCloser A special thing needed for property streaming
     */
    public CatalogFacade(
        Consumer<AutoCloseable> streamCloser,
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        ProcedureReturnColumns procedureReturnColumns,
        ProcedureTransactionService procedureTransactionService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContextService transactionContextService,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        UserLogServices userLogServices,
        CatalogBusinessFacade businessFacade
    ) {
        this.streamCloser = streamCloser;
        this.databaseId = databaseId;
        this.graphDatabaseService = graphDatabaseService;
        this.procedureReturnColumns = procedureReturnColumns;
        this.procedureTransactionService = procedureTransactionService;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.transactionContextService = transactionContextService;
        this.user = user;
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.userLogServices = userLogServices;

        this.businessFacade = businessFacade;
    }

    /**
     * Discussion: this is used by two stubs, with different output marshalling functions.
     * <p>
     * We know we should test {@link #graphExists(String)} in isolation because combinatorials.
     * <p>
     * Do we test the output marshallers?
     * <p>
     * Well if we need confidence, not for just box ticking.
     * Neo4j Procedure Framework requires POJOs of a certain shape,
     * so there is scope for writing ridiculous amounts of code if you fancy ticking boxes.
     */
    @SuppressWarnings("WeakerAccess")
    public <RETURN_TYPE> RETURN_TYPE graphExists(String graphName, Function<Boolean, RETURN_TYPE> outputMarshaller) {
        var graphExists = graphExists(graphName);

        return outputMarshaller.apply(graphExists);
    }

    public boolean graphExists(String graphName) {
        return businessFacade.graphExists(user, databaseId, graphName);
    }

    /**
     * Huh, we never did jobId filtering...
     */
    public Stream<UserLogEntry> queryUserLog(String jobId) {
        var userLogStore = userLogServices.getUserLogStore(databaseId);

        return userLogStore.query(user.getUsername());
    }

    /**
     * @param failIfMissing enable validation that graphs exist before dropping them
     * @param databaseName  optional override
     * @param username      optional override
     * @throws IllegalArgumentException if a database name was null or blank or not a String
     */
    public Stream<GraphInfo> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username
    ) throws IllegalArgumentException {
        var results = businessFacade.dropGraph(
            graphNameOrListOfGraphNames,
            failIfMissing,
            databaseName,
            username,
            databaseId,
            user
        );

        // we convert here from domain type to Neo4j display type
        return results.stream().map(gswc -> GraphInfo.withoutMemoryUsage(
            gswc.config(),
            gswc.graphStore()
        ));
    }

    public Stream<GraphInfoWithHistogram> listGraphs(String graphName) {
        graphName = validateValue(graphName);

        var displayDegreeDistribution = procedureReturnColumns.contains("degreeDistribution");

        var results = businessFacade.listGraphs(user, graphName, displayDegreeDistribution, terminationFlag);

        // we convert here from domain type to Neo4j display type
        var computeGraphSize = procedureReturnColumns.contains("memoryUsage")
            || procedureReturnColumns.contains("sizeInBytes");
        return results.stream().map(p -> GraphInfoWithHistogram.of(
            p.getLeft().config(),
            p.getLeft().graphStore(),
            p.getRight(),
            computeGraphSize
        ));
    }

    public Stream<GraphProjectNativeResult> nativeProject(
        String graphName,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> configuration
    ) {
        var transactionContext = transactionContextService.transactionContext(
            graphDatabaseService,
            procedureTransactionService
        );

        var result = businessFacade.nativeProject(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            graphName,
            nodeProjection,
            relationshipProjection,
            configuration
        );

        // the fact that it is a stream is just a Neo4j Procedure Framework convention
        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> estimateNativeProject(
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> configuration
    ) {
        var transactionContext = transactionContextService.transactionContext(
            graphDatabaseService,
            procedureTransactionService
        );

        var result = businessFacade.estimateNativeProject(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            nodeProjection,
            relationshipProjection,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<GraphProjectCypherResult> cypherProject(
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    ) {
        var transactionContext = transactionContextService.transactionContext(
            graphDatabaseService,
            procedureTransactionService
        );

        var result = businessFacade.cypherProject(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            graphName,
            nodeQuery,
            relationshipQuery,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> estimateCypherProject(
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    ) {
        var transactionContext = transactionContextService.transactionContext(
            graphDatabaseService,
            procedureTransactionService
        );

        var result = businessFacade.estimateCypherProject(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            nodeQuery,
            relationshipQuery,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<GraphFilterResult> subGraphProject(
        String graphName,
        String originGraphName,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.subGraphProject(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            originGraphName,
            nodeFilter,
            relationshipFilter,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<GraphMemoryUsage> sizeOf(String graphName) {
        var result = businessFacade.sizeOf(user, databaseId, graphName);

        return Stream.of(result);
    }

    public Stream<GraphDropNodePropertiesResult> dropNodeProperties(
        String graphName,
        Object nodeProperties,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.dropNodeProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            nodeProperties,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<GraphDropRelationshipResult> dropRelationships(
        String graphName,
        String relationshipType
    ) {
        var result = businessFacade.dropRelationships(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            relationshipType
        );

        return Stream.of(result);
    }

    public Stream<GraphDropGraphPropertiesResult> dropGraphProperty(
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    ) {
        var numberOfPropertiesRemoved = businessFacade.dropGraphProperty(
            user,
            databaseId,
            graphName,
            graphProperty,
            configuration
        );

        return Stream.of(new GraphDropGraphPropertiesResult(graphName, graphProperty, numberOfPropertiesRemoved));
    }

    public Stream<MutateLabelResult> mutateNodeLabel(
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.mutateNodeLabel(user, databaseId, graphName, nodeLabel, configuration);

        return Stream.of(result);
    }

    public Stream<StreamGraphPropertyResult> streamGraphProperty(
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.streamGraphProperty(
            user,
            databaseId,
            graphName,
            graphProperty,
            configuration
        );

        return result.map(StreamGraphPropertyResult::new);
    }

    public Stream<GraphStreamNodePropertiesResult> streamNodeProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    ) {
        return streamNodePropertyOrProperties(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            GraphStreamNodePropertiesResult::new
        );
    }

    public Stream<GraphStreamNodePropertyResult> streamNodeProperty(
        String graphName,
        String nodeProperty,
        Object nodeLabels,
        Map<String, Object> configuration
    ) {
        return streamNodePropertyOrProperties(
            graphName,
            List.of(nodeProperty),
            nodeLabels,
            configuration,
            (nodeId, propertyName, propertyValue, nodeLabelList) -> new GraphStreamNodePropertyResult(
                nodeId,
                propertyValue,
                nodeLabelList
            )
        );
    }

    // good generics!
    private <T> Stream<T> streamNodePropertyOrProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        // odd behaviour selection, how can we do better?
        var usesPropertyNameColumn = procedureReturnColumns.contains("nodeProperty");

        var resultStream = businessFacade.streamNodeProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );

        streamCloser.accept(resultStream);

        return resultStream;
    }

    public Stream<GraphStreamRelationshipPropertiesResult> streamRelationshipProperties(
        String graphName,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        Map<String, Object> configuration
    ) {
        return streamRelationshipPropertyOrProperties(
            graphName,
            relationshipProperties,
            relationshipTypes,
            configuration,
            GraphStreamRelationshipPropertiesResult::new
        );
    }

    public Stream<GraphStreamRelationshipPropertyResult> streamRelationshipProperty(
        String graphName,
        String relationshipProperty,
        List<String> relationshipTypes,
        Map<String, Object> configuration
    ) {
        return streamRelationshipPropertyOrProperties(
            graphName,
            List.of(relationshipProperty),
            relationshipTypes,
            configuration,
            (sourceId, targetId, relationshipType, propertyName, propertyValue) -> new GraphStreamRelationshipPropertyResult(
                sourceId,
                targetId,
                relationshipType,
                propertyValue
            )
        );
    }

    public Stream<TopologyResult> streamRelationships(
        String graphName,
        List<String> relationshipTypes,
        Map<String, Object> configuration
    ) {
        return businessFacade.streamRelationships(user, databaseId, graphName, relationshipTypes, configuration);
    }

    public Stream<NodePropertiesWriteResult> writeNodeProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.writeNodeProperties(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            graphName,
            nodeProperties,
            nodeLabels,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<WriteRelationshipPropertiesResult> writeRelationshipProperties(
        String graphName,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.writeRelationshipProperties(
            user,
            databaseId,
            terminationFlag,
            graphName,
            relationshipType,
            relationshipProperties,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<WriteLabelResult> writeNodeLabel(
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.writeNodeLabel(
            user,
            databaseId,
            terminationFlag,
            graphName,
            nodeLabel,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<WriteRelationshipResult> writeRelationships(
        String graphName,
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.writeRelationships(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            graphName,
            relationshipType,
            relationshipProperty,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<RandomWalkSamplingResult> sampleRandomWalkWithRestarts(
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.sampleRandomWalkWithRestarts(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            originGraphName,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<RandomWalkSamplingResult> sampleCommonNeighbourAwareRandomWalk(
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.sampleCommonNeighbourAwareRandomWalk(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            originGraphName,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> estimateCommonNeighbourAwareRandomWalk(
        String graphName,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.estimateCommonNeighbourAwareRandomWalk(
            user,
            databaseId,
            graphName,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<GraphGenerationStats> generateGraph(
        String graphName,
        long nodeCount,
        long averageDegree,
        Map<String, Object> configuration
    ) {
        var result = businessFacade.generateGraph(user, databaseId, graphName, nodeCount, averageDegree, configuration);

        return Stream.of(result);
    }

    private <T> Stream<T> streamRelationshipPropertyOrProperties(
        String graphName,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        Map<String, Object> configuration,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        var usesPropertyNameColumn = procedureReturnColumns.contains("relationshipProperty");

        var resultStream = businessFacade.streamRelationshipProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            relationshipProperties,
            relationshipTypes,
            configuration,
            usesPropertyNameColumn,
            outputMarshaller
        );

        streamCloser.accept(resultStream);

        return resultStream;
    }

    /**
     * We have to potentially unstack the placeholder. This is purely a Neo4j Procedure framework concern.
     */
    private String validateValue(String graphName) {
        if (NO_VALUE_PLACEHOLDER.equals(graphName)) return null;

        return graphName;
    }
}
