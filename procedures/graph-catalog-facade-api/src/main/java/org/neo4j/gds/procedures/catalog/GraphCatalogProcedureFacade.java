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

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.graphstorecatalog.DatabaseExportResult;
import org.neo4j.gds.applications.graphstorecatalog.FileExportResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphGenerationStats;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsage;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamNodePropertyResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.GraphStreamRelationshipPropertyResult;
import org.neo4j.gds.applications.graphstorecatalog.MutateLabelResult;
import org.neo4j.gds.applications.graphstorecatalog.NodePropertiesWriteResult;
import org.neo4j.gds.applications.graphstorecatalog.RandomWalkSamplingResult;
import org.neo4j.gds.applications.graphstorecatalog.TopologyResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteLabelResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipPropertiesResult;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipResult;
import org.neo4j.gds.beta.filter.GraphFilterResult;
import org.neo4j.gds.core.loading.GraphDropNodePropertiesResult;
import org.neo4j.gds.core.loading.GraphDropRelationshipResult;
import org.neo4j.gds.legacycypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.projection.GraphProjectNativeResult;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public interface GraphCatalogProcedureFacade {
    /**
     * This exists because procedures need default values sometimes.
     * For example, CALL gds.graph.list() would fail otherwise,
     * the user would have to do something silly like CALL gds.graph.list("")
     */
    String NO_VALUE_PLACEHOLDER = "d9b6394a-9482-4929-adab-f97df578a6c6";

    @SuppressWarnings("WeakerAccess")
    <RETURN_TYPE> RETURN_TYPE graphExists(String graphName, Function<Boolean, RETURN_TYPE> outputMarshaller);

    boolean graphExists(String graphName);

    Stream<GraphInfo> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username
    ) throws IllegalArgumentException;

    Stream<GraphInfoWithHistogram> listGraphs(String graphName);

    Stream<GraphProjectNativeResult> nativeProject(
        String graphName,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> estimateNativeProject(
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> configuration
    );

    Stream<GraphProjectCypherResult> cypherProject(
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> estimateCypherProject(
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> configuration
    );

    Stream<GraphFilterResult> subGraphProject(
        String graphName,
        String originGraphName,
        String nodeFilter,
        String relationshipFilter,
        Map<String, Object> configuration
    );

    Stream<GraphMemoryUsage> sizeOf(String graphName);

    Stream<GraphDropNodePropertiesResult> dropNodeProperties(
        String graphName,
        Object nodeProperties,
        Map<String, Object> configuration
    );

    Stream<GraphDropRelationshipResult> dropRelationships(
        String graphName,
        String relationshipType
    );

    Stream<GraphDropGraphPropertiesResult> dropGraphProperty(
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    Stream<MutateLabelResult> mutateNodeLabel(
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    Stream<StreamGraphPropertyResult> streamGraphProperty(
        String graphName,
        String graphProperty,
        Map<String, Object> configuration
    );

    Stream<GraphStreamNodePropertiesResult> streamNodeProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    );

    Stream<GraphStreamNodePropertyResult> streamNodeProperty(
        String graphName,
        String nodeProperty,
        Object nodeLabels,
        Map<String, Object> configuration
    );

    Stream<GraphStreamRelationshipPropertiesResult> streamRelationshipProperties(
        String graphName,
        List<String> relationshipProperties,
        Object relationshipTypes,
        Map<String, Object> configuration
    );

    Stream<GraphStreamRelationshipPropertyResult> streamRelationshipProperty(
        String graphName,
        String relationshipProperty,
        Object relationshipTypes,
        Map<String, Object> configuration
    );

    Stream<TopologyResult> streamRelationships(
        String graphName,
        Object relationshipTypes,
        Map<String, Object> configuration
    );

    Stream<NodePropertiesWriteResult> writeNodeProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration
    );

    Stream<WriteRelationshipPropertiesResult> writeRelationshipProperties(
        String graphName,
        String relationshipType,
        List<String> relationshipProperties,
        Map<String, Object> configuration
    );

    Stream<WriteLabelResult> writeNodeLabel(
        String graphName,
        String nodeLabel,
        Map<String, Object> configuration
    );

    Stream<WriteRelationshipResult> writeRelationships(
        String graphName,
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> configuration
    );

    Stream<RandomWalkSamplingResult> sampleRandomWalkWithRestarts(
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    Stream<RandomWalkSamplingResult> sampleCommonNeighbourAwareRandomWalk(
        String graphName,
        String originGraphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> estimateCommonNeighbourAwareRandomWalk(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<FileExportResult> exportToCsv(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> exportToCsvEstimate(String graphName, Map<String, Object> configuration);

    Stream<DatabaseExportResult> exportToDatabase(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<GraphGenerationStats> generateGraph(
        String graphName,
        long nodeCount,
        long averageDegree,
        Map<String, Object> configuration
    );
}
