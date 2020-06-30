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
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphDropProc extends CatalogProc {

    private static final String DESCRIPTION = "Drops a named graph from the catalog and frees up the resources it occupies.";

    @Procedure(name = "gds.graph.drop", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphDropInfo> drop(@Name(value = "graphName") String graphName) {
        validateGraphName(graphName);

        AtomicReference<GraphDropInfo> result = new AtomicReference<>();
        GraphStoreCatalog.remove(getUsername(), graphName, (graphStoreWithConfig) -> {
            result.set(new GraphDropInfo(graphStoreWithConfig.config(), graphStoreWithConfig.graphStore()));
        });

        return Stream.of(result.get());
    }

    public static class GraphDropInfo {
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

        GraphDropInfo(
            GraphCreateConfig config,
            GraphStore graphStore
        ) {
            GraphInfo graphInfo = new GraphInfo(config, graphStore, false);

            this.graphName = graphInfo.graphName;
            this.memoryUsage = graphInfo.memoryUsage;
            this.sizeInBytes = graphInfo.sizeInBytes;
            this.nodeProjection = graphInfo.nodeProjection;
            this.relationshipProjection = graphInfo.relationshipProjection;
            this.nodeQuery = graphInfo.nodeQuery;
            this.relationshipQuery = graphInfo.relationshipQuery;
            this.nodeCount = graphInfo.nodeCount;
            this.relationshipCount = graphInfo.relationshipCount;
            this.creationTime = graphInfo.creationTime;
            this.modificationTime = graphInfo.modificationTime;
            this.schema = graphInfo.schema;
        }
    }
}
