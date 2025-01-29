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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;

import java.time.ZonedDateTime;
import java.util.Map;

@SuppressWarnings("unused")
public class GraphInfoWithHistogram extends GraphInfo {

    public final Map<String, Object> degreeDistribution;

    /**
     * Canonical constructor which is used for deserialization
     */
    public GraphInfoWithHistogram(
        @JsonProperty("graphName") String graphName,
        @JsonProperty("database") String database,
        @JsonProperty("databaseLocation") String databaseLocation,
        @JsonProperty("configuration") Map<String, Object> configuration,
        @JsonProperty("memoryUsage") String memoryUsage,
        @JsonProperty("sizeInBytes") long sizeInBytes,
        @JsonProperty("nodeCount") long nodeCount,
        @JsonProperty("relationshipCount") long relationshipCount,
        @JsonProperty("creationTime") ZonedDateTime creationTime,
        @JsonProperty("modificationTime") ZonedDateTime modificationTime,
        @JsonProperty("schema") Map<String, Object> schema,
        @JsonProperty("schemaWithOrientation") Map<String, Object> schemaWithOrientation,
        @JsonProperty("degreeDistribution") Map<String, Object> degreeDistribution
    ) {
        super(
            graphName,
            database,
            databaseLocation,
            configuration,
            memoryUsage,
            sizeInBytes,
            nodeCount,
            relationshipCount,
            creationTime,
            modificationTime,
            schema,
            schemaWithOrientation
        );
        this.degreeDistribution = degreeDistribution;
    }

    public GraphInfoWithHistogram(
        GraphInfo graphInfo,
        Map<String, Object> degreeDistribution
    ) {
        this(
            graphInfo.graphName,
            graphInfo.database,
            graphInfo.databaseLocation,
            graphInfo.configuration,
            graphInfo.memoryUsage,
            graphInfo.sizeInBytes,
            graphInfo.nodeCount,
            graphInfo.relationshipCount,
            graphInfo.creationTime,
            graphInfo.modificationTime,
            graphInfo.schema,
            graphInfo.schemaWithOrientation,
            degreeDistribution
        );
    }

    /**
     * @param degreeDistribution null implies not including it
     * @param computeGraphSize   selects what kind of GraphInfo to create
     */
    public static GraphInfoWithHistogram of(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        Map<String, Object> degreeDistribution,
        boolean computeGraphSize
    ) {
        var graphInfo = computeGraphSize
            ? withMemoryUsage(graphProjectConfig, graphStore)
            : withoutMemoryUsage(graphProjectConfig, graphStore);

        return new GraphInfoWithHistogram(graphInfo, degreeDistribution);
    }
}
