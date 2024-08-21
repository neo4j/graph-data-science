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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.DegreeDistribution;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.mem.Estimate;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;

public class GraphInfo {

    public final String graphName;
    public final String database;
    public final String databaseLocation;
    public final String memoryUsage;
    public final long sizeInBytes;
    public final long nodeCount;
    public final long relationshipCount;
    public final Map<String, Object> configuration;
    public final double density;
    public final ZonedDateTime creationTime;
    public final ZonedDateTime modificationTime;

    @Deprecated(forRemoval = true)
    public final Map<String, Object> schema;
    public final Map<String, Object> schemaWithOrientation;

    GraphInfo(
        String graphName,
        String database,
        String databaseLocation,
        Map<String, Object> configuration,
        String memoryUsage,
        long sizeInBytes,
        long nodeCount,
        long relationshipCount,
        ZonedDateTime creationTime,
        ZonedDateTime modificationTime,
        Map<String, Object> schema,
        Map<String, Object> schemaWithOrientation
    ) {
        this.graphName = graphName;
        this.database = database;
        this.databaseLocation = databaseLocation;
        this.memoryUsage = memoryUsage;
        this.sizeInBytes = sizeInBytes;
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.density = DegreeDistribution.density(nodeCount, relationshipCount);
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
        this.schema = schema;
        this.schemaWithOrientation = schemaWithOrientation;
        this.configuration = configuration;
    }

    public static GraphInfo withMemoryUsage(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore
    ) {
        var sizeInBytes = MemoryUsage.sizeOf(graphStore);

        var memoryUsage = sizeInBytes >= 0
            ? Estimate.humanReadable(sizeInBytes)
            : null;

        return create(
            graphProjectConfig,
            graphStore,
            memoryUsage,
            sizeInBytes
        );
    }

    public static GraphInfo withoutMemoryUsage(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore
    ) {
        return create(
            graphProjectConfig,
            graphStore,
            "",
            -1L
        );
    }

    private static GraphInfo create(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        String memoryUsage,
        long sizeInBytes
    ) {
        var configurationMap = graphProjectConfig.asProcedureResultConfigurationField();

        return new GraphInfo(
            graphProjectConfig.graphName(),
            graphStore.databaseInfo().databaseId().databaseName(),
            graphStore.databaseInfo().databaseLocation().name().toLowerCase(Locale.ROOT),
            configurationMap,
            memoryUsage,
            sizeInBytes,
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            graphStore.creationTime(),
            graphStore.modificationTime(),
            graphStore.schema().toMapOld(),
            graphStore.schema().toMap()
        );
    }
}
