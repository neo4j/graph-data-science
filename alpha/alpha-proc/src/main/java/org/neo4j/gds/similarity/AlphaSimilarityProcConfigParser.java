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

import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.executor.ProcConfigParser;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.gds.similarity.nil.NullGraphStore;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;

import static org.neo4j.gds.config.GraphProjectFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.gds.similarity.AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME;
import static org.neo4j.gds.similarity.AlphaSimilarityProc.removeGraph;

public class AlphaSimilarityProcConfigParser<CONFIG extends SimilarityConfig> implements ProcConfigParser<CONFIG> {

    private final String username;
    private final ProcConfigParser<CONFIG> configParser;
    private final NamedDatabaseId databaseId;

    AlphaSimilarityProcConfigParser(
        String username,
        ProcConfigParser<CONFIG> configParser,
        NamedDatabaseId databaseId
    ) {
        this.username = username;
        this.configParser = configParser;
        this.databaseId = databaseId;
    }

    @Override
    public CONFIG processInput(Map<String, Object> configuration) {
        // We will tell the rest of the system that we are in named graph mode, with a fake graph name
        var graphName = SIMILARITY_FAKE_GRAPH_NAME;

        // We must curate the configuration map to remove any eventual projection keys
        // This is backwards compatibility since the alpha similarities featured anonymous star projections in docs
        configuration.remove(NODE_QUERY_KEY);
        configuration.remove(RELATIONSHIP_QUERY_KEY);
        configuration.remove(NODE_PROJECTION_KEY);
        configuration.remove(RELATIONSHIP_PROJECTION_KEY);

        // We put the fake graph store into the graph catalog
        GraphStoreCatalog.set(
            ImmutableGraphProjectFromStoreConfig.of(
                username,
                graphName,
                NodeProjections.ALL,
                RelationshipProjections.ALL
            ),
            new NullGraphStore(databaseId)
        );
        // And finally we call super in named graph mode
        try {
            return configParser.processInput(configuration);
        } catch (RuntimeException e) {
            removeGraph(username, this.databaseId);
            throw e;
        }
    }
}
