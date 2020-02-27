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

import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.export.NeoExport;
import org.neo4j.graphalgo.core.utils.export.NeoExportConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphExportProc extends BaseProc {

    @Procedure(name = "gds.alpha.graph.export", mode = READ)
    @Description("Exports a named graph into a new Neo4j database.")
    public Stream<GraphExportResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        NeoExportConfig config = NeoExportConfig.of(getUsername(), cypherConfig);
        validateConfig(cypherConfig, config);

        GraphExportResult result = runWithExceptionLogging(
            "Graph creation failed", () -> {
                Graph graph = GraphStoreCatalog.get(getUsername(), graphName).getGraph();
                NeoExport neoExport = new NeoExport(graph, config);

                long start = System.nanoTime();
                neoExport.run();
                long end = System.nanoTime();

                return new GraphExportResult(
                    graphName,
                    config.storeDir(),
                    config.dbName(),
                    graph.nodeCount(),
                    graph.relationshipCount(),
                    graph.availableNodeProperties().size() * graph.nodeCount(),
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
                );
            }
        );

        return Stream.of(result);
    }

    public static class GraphExportResult {
        public final String graphName;
        public final String storeDir;
        public final String dbName;
        public final long nodeCount;
        public final long relationshipCount;
        public final long nodePropertyCount;
        public final long writeMillis;

        public GraphExportResult(
            String graphName,
            String storeDir,
            String dbName,
            long nodeCount,
            long relationshipCount,
            long nodePropertyCount,
            long writeMillis
        ) {
            this.graphName = graphName;
            this.storeDir = storeDir;
            this.dbName = dbName;
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.nodePropertyCount = nodePropertyCount;
            this.writeMillis = writeMillis;
        }
    }
}
