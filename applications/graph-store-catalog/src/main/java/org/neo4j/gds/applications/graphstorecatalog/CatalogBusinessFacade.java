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
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.beta.filter.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.legacycypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface CatalogBusinessFacade {
    boolean graphExists(User user, DatabaseId databaseId, String graphNameAsString);

    List<GraphStoreCatalogEntry> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username,
        DatabaseId currentDatabase,
        User operator
    );

    List<Pair<GraphStoreCatalogEntry, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    );

    GraphProjectNativeResult nativeProject(
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
    );

    MemoryEstimateResult estimateNativeProject(
        DatabaseId databaseId,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
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
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
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
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    );

    GraphFilterResult subGraphProject(
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
        Map<String, Object> configuration
    );

    GraphDropRelationshipResult dropRelationships(
        User user, DatabaseId databaseId, TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String relationshipType
    );

    long dropGraphProperty(
        User user, DatabaseId databaseId, String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    MutateLabelResult mutateNodeLabel(
        User user,
        DatabaseId databaseId,
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    Stream<?> streamGraphProperty(
        User user,
        DatabaseId databaseId,
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    <T> Stream<T> streamNodeProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    );

    <T> Stream<T> streamRelationshipProperties(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    );

    Stream<TopologyResult> streamRelationships(
        User user,
        DatabaseId databaseId,
        String graphName,
        List<String> relationshipTypes,
        Map<String, Object> configuration
    );

    NodePropertiesWriteResult writeNodeProperties(
        User user,
        DatabaseId databaseId,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    );

    WriteRelationshipPropertiesResult writeRelationshipProperties(
        User user,
        DatabaseId databaseId,
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        TerminationFlag terminationFlag,
        String graphName,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> configuration
    );

    WriteLabelResult writeNodeLabel(
        User user,
        DatabaseId databaseId,
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        TerminationFlag terminationFlag,
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    WriteRelationshipResult writeRelationships(
        User user,
        DatabaseId databaseId,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> configuration
    );

    RandomWalkSamplingResult sampleRandomWalkWithRestarts(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    RandomWalkSamplingResult sampleCommonNeighbourAwareRandomWalk(
        User user,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    MemoryEstimateResult estimateCommonNeighbourAwareRandomWalk(
        User user,
        DatabaseId databaseId,
        String graphName,
        Map<String, Object> configuration
    );

    GraphGenerationStats generateGraph(
        User user,
        DatabaseId databaseId,
        String graphName,
        long nodeCount,
        long averageDegree,
        Map<String, Object> configuration
    );
}
