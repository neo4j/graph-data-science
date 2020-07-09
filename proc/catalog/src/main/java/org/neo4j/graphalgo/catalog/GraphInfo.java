/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.catalog;

import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.time.ZonedDateTime;
import java.util.Map;

public class GraphInfo {

    public final String graphName;
    public final String memoryUsage;
    public final long sizeInBytes;
    public final Map<String, Object> nodeProjection;
    public final Map<String, Object> relationshipProjection;
    public final String nodeQuery;
    public final String relationshipQuery;
    public final long nodeCount;
    public final long relationshipCount;
    public final ZonedDateTime creationTime;
    public final ZonedDateTime modificationTime;
    public final Map<String, Object> schema;

    GraphInfo(
        String graphName,
        String memoryUsage,
        long sizeInBytes,
        Map<String, Object> nodeProjection,
        Map<String, Object> relationshipProjection,
        String nodeQuery,
        String relationshipQuery,
        long nodeCount,
        long relationshipCount,
        ZonedDateTime creationTime,
        ZonedDateTime modificationTime,
        Map<String, Object> schema
    ) {
        this.graphName = graphName;
        this.memoryUsage = memoryUsage;
        this.sizeInBytes = sizeInBytes;
        this.nodeProjection = nodeProjection;
        this.relationshipProjection = relationshipProjection;
        this.nodeQuery = nodeQuery;
        this.relationshipQuery = relationshipQuery;
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
        this.schema = schema;
    }

    static GraphInfo of(GraphCreateConfig graphCreateConfig, GraphStore graphStore) {
        var graphName = graphCreateConfig.graphName();
        var creationTime = graphCreateConfig.creationTime();

        String nodeQuery;
        String relationshipQuery;
        Map<String, Object> nodeProjection;
        Map<String, Object> relationshipProjection;

        if (graphCreateConfig instanceof GraphCreateFromCypherConfig) {
            GraphCreateFromCypherConfig cypherConfig = (GraphCreateFromCypherConfig) graphCreateConfig;
            nodeQuery = cypherConfig.nodeQuery();
            relationshipQuery = cypherConfig.relationshipQuery();
            nodeProjection = null;
            relationshipProjection = null;
        } else if (graphCreateConfig instanceof RandomGraphGeneratorConfig) {
            RandomGraphGeneratorConfig randomGraphConfig = (RandomGraphGeneratorConfig) graphCreateConfig;
            nodeProjection = randomGraphConfig.nodeProjections().toObject();
            relationshipProjection = randomGraphConfig.relationshipProjections().toObject();
            nodeQuery = null;
            relationshipQuery = null;
        } else {
            GraphCreateFromStoreConfig fromStoreConfig = (GraphCreateFromStoreConfig) graphCreateConfig;
            nodeProjection = fromStoreConfig.nodeProjections().toObject();
            relationshipProjection = fromStoreConfig.relationshipProjections().toObject();
            nodeQuery = null;
            relationshipQuery = null;
        }

        var modificationTime = graphStore.modificationTime();
        var nodeCount = graphStore.nodeCount();
        var relationshipCount = graphStore.relationshipCount();
        var schema = graphStore.schema().toMap();
        var sizeInBytes = MemoryUsage.sizeOf(graphStore);
        var memoryUsage = MemoryUsage.humanReadable(sizeInBytes);

        return new GraphInfo(
            graphName,
            memoryUsage,
            sizeInBytes,
            nodeProjection,
            relationshipProjection,
            nodeQuery,
            relationshipQuery,
            nodeCount,
            relationshipCount,
            creationTime,
            modificationTime,
            schema
        );
    }
}
