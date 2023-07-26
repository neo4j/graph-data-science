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
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.ConfigurationService;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphProjectNativeResult;
import org.neo4j.gds.core.loading.GraphProjectSubgraphResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.List;
import java.util.Map;

/**
 * This layer is shared between Neo4j and other integrations. It is entry-point agnostic.
 * "Business facade" to distinguish it from "procedure facade" and similar.
 * <p>
 * Here we have just business logic: no Neo4j bits or other integration bits, just Java POJO things.
 * <p>
 * By nature business logic is going to be bespoke, so one method per logical thing.
 * Take {@link DefaultGraphStoreCatalogBusinessFacade#graphExists(User, DatabaseId, String)} for example:
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
public class DefaultGraphStoreCatalogBusinessFacade implements GraphStoreCatalogBusinessFacade {
    // services
    private final ConfigurationService configurationService;
    private final GraphNameValidationService graphNameValidationService;

    // business logic
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DropGraphService dropGraphService;
    private final ListGraphService listGraphService;
    private final NativeProjectService nativeProjectService;
    private final CypherProjectService cypherProjectService;
    private final SubGraphProjectService subGraphProjectService;
    private final GraphMemoryUsageService graphMemoryUsageService;

    public DefaultGraphStoreCatalogBusinessFacade(
        ConfigurationService configurationService,
        GraphNameValidationService graphNameValidationService,
        GraphStoreCatalogService graphStoreCatalogService,
        DropGraphService dropGraphService,
        ListGraphService listGraphService,
        NativeProjectService nativeProjectService,
        CypherProjectService cypherProjectService,
        SubGraphProjectService subGraphProjectService,
        GraphMemoryUsageService graphMemoryUsageService
    ) {
        this.configurationService = configurationService;
        this.graphNameValidationService = graphNameValidationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.dropGraphService = dropGraphService;
        this.listGraphService = listGraphService;
        this.nativeProjectService = nativeProjectService;
        this.cypherProjectService = cypherProjectService;
        this.subGraphProjectService = subGraphProjectService;
        this.graphMemoryUsageService = graphMemoryUsageService;
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

        return dropGraphService.compute(validatedGraphNames, failIfMissing, databaseId, operator, usernameOverride);
    }

    @Override
    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        var validatedGraphName = graphNameValidationService.validatePossibleNull(graphName);

        return listGraphService.list(user, validatedGraphName, includeDegreeDistribution, terminationFlag);
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
        var graphName = validateGraphNameValidAndUnknown(user, databaseId, graphNameAsString);

        var configuration = configurationService.parseNativeProjectConfiguration(
            user,
            graphName,
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        );

        return nativeProjectService.project(
            databaseId,
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
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = configurationService.parseEstimateNativeProjectConfiguration(
            nodeProjection,
            relationshipProjection,
            rawConfiguration
        );

        return nativeProjectService.estimate(
            databaseId,
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
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var graphName = validateGraphNameValidAndUnknown(user, databaseId, graphNameAsString);

        var configuration = configurationService.parseCypherProjectConfiguration(
            user,
            graphName,
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        );

        return cypherProjectService.project(
            databaseId,
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
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = configurationService.parseEstimateCypherProjectConfiguration(
            nodeQuery,
            relationshipQuery,
            rawConfiguration
        );

        return cypherProjectService.estimate(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    @Override
    public GraphProjectSubgraphResult subGraphProject(
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
        var graphName = validateGraphNameValidAndUnknown(user, databaseId, graphNameAsString);
        var originGraphName = graphNameValidationService.validate(originGraphNameAsString);

        var originGraphConfiguration = graphStoreCatalogService.get(
            CatalogRequest.of(user.getUsername(), databaseId),
            originGraphName
        );

        var configuration = configurationService.parseSubGraphProjectConfiguration(
            user,
            graphName,
            originGraphName,
            nodeFilter,
            relationshipFilter,
            originGraphConfiguration,
            rawConfiguration
        );

        return subGraphProjectService.project(
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

        return graphMemoryUsageService.sizeOf(
            user,
            databaseId,
            graphName
        );
    }

    private GraphName validateGraphNameValidAndUnknown(User user, DatabaseId databaseId, String graphNameAsString) {
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
