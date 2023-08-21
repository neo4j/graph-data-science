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
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * We want to ensure that, no matter which business method is called, then preconditions get checked.
 * This decorator does that: it sits in front of the business layer and basically does nothing else.
 * This aids auditing, I can eyeball that each method does the little indirection.
 * If one day some business method needs to opt out from preconditions checks, we can implement that as a pass-through.
 * De-clutters the business facade where before, each method had one method called sprinkled at the top.
 */
public class GraphStoreCatalogBusinessFacadePreConditionsDecorator implements GraphStoreCatalogBusinessFacade {
    private final GraphStoreCatalogBusinessFacade delegate;
    private final PreconditionsService preconditionsService;

    public GraphStoreCatalogBusinessFacadePreConditionsDecorator(
        GraphStoreCatalogBusinessFacade delegate,
        PreconditionsService preconditionsService
    ) {
        this.delegate = delegate;
        this.preconditionsService = preconditionsService;
    }

    @Override
    public boolean graphExists(User user, DatabaseId databaseId, String graphNameAsString) {
        return runWithPreconditionsChecked(() -> delegate.graphExists(user, databaseId, graphNameAsString));
    }

    @Override
    public List<GraphStoreWithConfig> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username,
        DatabaseId currentDatabase,
        User operator
    ) {
        return runWithPreconditionsChecked(() -> delegate.dropGraph(
            graphNameOrListOfGraphNames,
            failIfMissing,
            databaseName,
            username,
            currentDatabase,
            operator
        ));
    }

    @Override
    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        return runWithPreconditionsChecked(() -> delegate.listGraphs(
            user,
            graphName,
            includeDegreeDistribution,
            terminationFlag
        ));
    }

    @Override
    public GraphProjectNativeResult nativeProject(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        return runWithPreconditionsChecked(() -> delegate.nativeProject(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            graphNameAsString,
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        ));
    }

    @Override
    public MemoryEstimateResult estimateNativeProject(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        return runWithPreconditionsChecked(() -> delegate.estimateNativeProject(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        ));
    }

    @Override
    public GraphProjectCypherResult cypherProject(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.cypherProject(
            user,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            graphNameAsString,
            nodeQuery,
            relationshipQuery,
            configuration
        ));
    }

    @Override
    public MemoryEstimateResult estimateCypherProject(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        return runWithPreconditionsChecked(() -> delegate.estimateCypherProject(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        ));
    }

    @Override
    public GraphFilterResult subGraphProject(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String originGraphName,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.subGraphProject(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            originGraphName,
            nodeFilter,
            relationshipFilter,
            configuration
        ));
    }

    @Override
    public GraphMemoryUsage sizeOf(User user, DatabaseId databaseId, String graphName) {
        return runWithPreconditionsChecked(() -> delegate.sizeOf(user, databaseId, graphName));
    }

    @Override
    public GraphDropNodePropertiesResult dropNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        Object nodeProperties,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.dropNodeProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            nodeProperties,
            configuration
        ));
    }

    @Override
    public GraphDropRelationshipResult dropRelationships(
        User user, DatabaseId databaseId, TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String relationshipType,
        Optional<String> deprecationWarning
    ) {
        return runWithPreconditionsChecked(() -> delegate.dropRelationships(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            relationshipType,
            deprecationWarning
        ));
    }

    @Override
    public long dropGraphProperty(
        User user,
        DatabaseId databaseId,
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.dropGraphProperty(
            user,
            databaseId,
            graphName,
            graphProperty,
            configuration
        ));
    }

    @Override
    public MutateLabelResult mutateNodeLabel(
        User user,
        DatabaseId databaseId,
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.mutateNodeLabel(
            user,
            databaseId,
            graphName,
            nodeLabel,
            configuration
        ));
    }

    @Override
    public Stream<?> streamGraphProperty(
        User user,
        DatabaseId databaseId,
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.streamGraphProperty(
            user,
            databaseId,
            graphName,
            graphProperty,
            configuration
        ));
    }

    @Override
    public <T> Stream<T> streamNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        Optional<String> deprecationWarning,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        return runWithPreconditionsChecked(() -> delegate.streamNodeProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            usesPropertyNameColumn,
            deprecationWarning,
            outputMarshaller
        ));
    }

    @Override
    public <T> Stream<T> streamRelationshipProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        Optional<String> deprecationWarning,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        return runWithPreconditionsChecked(() -> delegate.streamRelationshipProperties(
            user,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory,
            graphName,
            relationshipProperties,
            relationshipTypes,
            configuration,
            usesPropertyNameColumn,
            deprecationWarning,
            outputMarshaller
        ));
    }

    @Override
    public Stream<TopologyResult> streamRelationships(
        User user,
        DatabaseId databaseId,
        String graphName,
        List<String> relationshipTypes,
        Map<String, Object> configuration
    ) {
        return runWithPreconditionsChecked(() -> delegate.streamRelationships(
            user,
            databaseId,
            graphName,
            relationshipTypes,
            configuration
        ));
    }

    private <T> T runWithPreconditionsChecked(Supplier<T> businessLogic) {
        preconditionsService.checkPreconditions();

        return businessLogic.get();
    }
}
