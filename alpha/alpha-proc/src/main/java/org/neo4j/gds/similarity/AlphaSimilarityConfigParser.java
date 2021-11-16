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
package org.neo4j.gds.similarity;

import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.gds.ConfigParser;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.gds.similarity.nil.NullGraphStore;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.gds.similarity.AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME;
import static org.neo4j.gds.similarity.AlphaSimilarityProc.removeGraph;

public class AlphaSimilarityConfigParser<CONFIG extends SimilarityConfig> extends ConfigParser<CONFIG> {

    private final NamedDatabaseId databaseId;
    private final NewConfigFunction<CONFIG> newConfigFunction;

    interface NewConfigFunction<CONFIG extends SimilarityConfig> {
        CONFIG apply(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        );
    }

    AlphaSimilarityConfigParser(
        String username,
        NamedDatabaseId databaseId,
        NewConfigFunction<CONFIG> newConfigFunction
    ) {
        super(username);
        this.databaseId = databaseId;
        this.newConfigFunction = newConfigFunction;
    }

    @Override
    protected CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return this.newConfigFunction.apply(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    public Pair<CONFIG, Optional<String>> processInput(
        Object graphNameOrConfig, Map<String, Object> configuration
    ) {
        if (graphNameOrConfig instanceof String) {
            throw new IllegalArgumentException("Similarity algorithms do not support named graphs");
        } else if (graphNameOrConfig instanceof Map) {
            // User is doing the only supported thing: anonymous syntax

            Map<String, Object> configMap = (Map<String, Object>) graphNameOrConfig;

            // We will tell the rest of the system that we are in named graph mode, with a fake graph name
            graphNameOrConfig = SIMILARITY_FAKE_GRAPH_NAME;
            // We move the map to the second argument position of CALL gds.algo.mode(name, config)
            configuration = configMap;

            // We must curate the configuration map to remove any eventual projection keys
            // This is backwards compatibility since the alpha similarities featured anonymous star projections in docs
            configuration.remove(NODE_QUERY_KEY);
            configuration.remove(RELATIONSHIP_QUERY_KEY);
            configuration.remove(NODE_PROJECTION_KEY);
            configuration.remove(RELATIONSHIP_PROJECTION_KEY);

            // We put the fake graph store into the graph catalog
            GraphStoreCatalog.set(
                ImmutableGraphCreateFromStoreConfig.of(
                    username,
                    graphNameOrConfig.toString(),
                    NodeProjections.ALL,
                    RelationshipProjections.ALL
                ),
                new NullGraphStore(databaseId)
            );
        }
        // And finally we call super in named graph mode
        try {
            return super.processInput(graphNameOrConfig, configuration);
        } catch (RuntimeException e) {
            removeGraph(this.username, this.databaseId);
            throw e;
        }
    }
}
