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
package org.neo4j.graphalgo.beta;

import org.neo4j.graphalgo.beta.filter.GraphStoreFilter;
import org.neo4j.graphalgo.beta.filter.GraphStoreFilterConfig;
import org.neo4j.graphalgo.beta.filter.expression.SemanticErrors;
import org.neo4j.graphalgo.catalog.CatalogProc;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromGraphConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphSubgraphProc extends CatalogProc {

    @Procedure(name = "gds.beta.graph.subgraph", mode = READ)
    public Stream<GraphSubgraphResult> generate(
        @Name(value = "graphName") String graphName,
        @Name(value = "subgraphName") String subgraphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(username(), subgraphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStoreFilterConfig config = GraphStoreFilterConfig.of(
            username(),
            graphName,
            subgraphName,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        GraphSubgraphResult result = runWithExceptionLogging(
            "Subgraph creation failed",
            ExceptionUtil.supplier(() -> compute(config)
            )
        );

        return Stream.of(result);
    }

    private GraphSubgraphResult compute(GraphStoreFilterConfig config) throws ParseException, SemanticErrors {
        var progressTimer = ProgressTimer.start();

        var graphStore = GraphStoreCatalog.get(username(), databaseId(), config.graphName());
        var subGraphStore = GraphStoreFilter.filter(
            graphStore.graphStore(),
            config,
            Pools.DEFAULT,
            allocationTracker()
        );

        var subgraphCreateConfig = ImmutableGraphCreateFromGraphConfig.builder()
            .username(username())
            .graphName(config.subgraphName())
            .nodeFilter(config.nodeFilter())
            .relationshipFilter(config.relationshipFilter())
            .originalConfig(graphStore.config())
            .build();

        GraphStoreCatalog.set(subgraphCreateConfig, subGraphStore);

        var createMillis = progressTimer.stop().getDuration();

        return new GraphSubgraphResult(
            subGraphStore.nodeCount(),
            subGraphStore.relationshipCount(),
            createMillis,
            config.toMap()
        );
    }

    public static class GraphSubgraphResult {

        public final long nodeCount;
        public final long relationshipCount;
        public final long createMillis;
        public final Map<String, Object> configuration;

        GraphSubgraphResult(
            long nodeCount,
            long relationshipCount,
            long createMillis,
            Map<String, Object> configuration
        ) {

            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.createMillis = createMillis;
            this.configuration = configuration;
        }
    }
}
