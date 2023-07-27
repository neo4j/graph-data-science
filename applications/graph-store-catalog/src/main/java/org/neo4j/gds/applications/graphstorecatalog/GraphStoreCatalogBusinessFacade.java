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
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphProjectNativeResult;
import org.neo4j.gds.core.loading.GraphProjectSubgraphResult;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface GraphStoreCatalogBusinessFacade {
    boolean graphExists(User user, DatabaseId databaseId, String graphNameAsString);

    List<GraphStoreWithConfig> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username,
        DatabaseId currentDatabase,
        User operator
    );

    List<Pair<GraphStoreWithConfig, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    );

    GraphProjectNativeResult nativeProject(
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
    );

    MemoryEstimateResult estimateNativeProject(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    );

    GraphProjectCypherResult cypherProject(
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
    );

    MemoryEstimateResult estimateCypherProject(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    );

    GraphProjectSubgraphResult subGraphProject(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphNameAsString,
        String originGraphNameAsString,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> configuration
    );

    GraphMemoryUsage sizeOf(User user, DatabaseId databaseId, String graphName);

    GraphDropNodePropertiesResult dropNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        Object nodeProperties,
        Map<String, Object> configuration,
        Optional<String> deprecationWarning
    );
}
