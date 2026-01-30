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
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.beta.filter.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.legacycypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface GraphCatalogApplications {
    boolean graphExists(RequestScopedDependencies requestScopedDependencies, String graphNameAsString);

    List<GraphStoreCatalogEntry> dropGraph(
        RequestScopedDependencies requestScopedDependencies,
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseNameOverride,
        String usernameOverride
    );

    List<Pair<GraphStoreCatalogEntry, Map<String, Object>>> listGraphs(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        boolean includeDegreeDistribution
    );

    GraphProjectNativeResult nativeProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String graphNameAsString,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    );

    MemoryEstimateResult estimateNativeProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    );

    GraphProjectCypherResult cypherProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String graphNameAsString,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    );

    MemoryEstimateResult estimateCypherProject(
        RequestScopedDependencies requestScopedDependencies,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TransactionContext transactionContext,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    );

    GraphFilterResult subGraphProject(
        RequestScopedDependencies requestScopedDependencies,
        String graphNameAsString,
        String originGraphNameAsString,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> configuration
    );

    GraphMemoryUsage sizeOf(RequestScopedDependencies requestScopedDependencies, String graphName);

    GraphDropNodePropertiesResult dropNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Object nodeProperties,
        Map<String, Object> configuration
    );

    GraphDropRelationshipResult dropRelationships(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String relationshipType
    );

    long dropGraphProperty(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    MutateLabelResult mutateNodeLabel(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    Stream<?> streamGraphProperty(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    <T> Stream<T> streamNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    );

    <T> Stream<T> streamRelationshipProperties(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        List<String> relationshipProperties,
        Object relationshipTypes,
        Map<String, Object> configuration,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    );

    Stream<TopologyResult> streamRelationships(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Object relationshipTypes,
        Map<String, Object> configuration
    );

    NodePropertiesWriteResult writeNodeProperties(
        RequestScopedDependencies requestScopedDependencies,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    );

    WriteRelationshipPropertiesResult writeRelationshipProperties(
        RequestScopedDependencies requestScopedDependencies,
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        String graphName,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> configuration
    );

    WriteLabelResult writeNodeLabel(
        RequestScopedDependencies requestScopedDependencies,
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    WriteRelationshipResult writeRelationships(
        RequestScopedDependencies requestScopedDependencies,
        RelationshipExporterBuilder relationshipExporterBuilder,
        String graphName,
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> configuration
    );

    RandomWalkSamplingResult sampleRandomWalkWithRestarts(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    RandomWalkSamplingResult sampleCommonNeighbourAwareRandomWalk(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    MemoryEstimateResult estimateCommonNeighbourAwareRandomWalk(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Map<String, Object> configuration
    );

    GraphGenerationStats generateGraph(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        long nodeCount,
        long averageDegree,
        Map<String, Object> configuration
    );

    FileExportResult exportToCsv(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Map<String, Object> configuration
    );

    MemoryEstimateResult exportToCsvEstimate(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Map<String, Object> configuration
    );

    DatabaseExportResult exportToDatabase(
        RequestScopedDependencies requestScopedDependencies,
        String graphName,
        Map<String, Object> configuration
    );
}
