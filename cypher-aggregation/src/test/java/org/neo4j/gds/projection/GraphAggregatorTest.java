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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class GraphAggregatorTest {

    @Test
    void shouldImportHighNodeIds() {
        var userName = "neo4j";
        var graphName = "graph";
        var databaseId = DatabaseId.random();

        var aggregator = new CypherAggregation.GraphAggregator(ProgressTimer.start(), databaseId, userName);

        long source = 1L << 50;
        long target = (1L << 50) + 1;

        aggregator.update(graphName, source, target, new HashMap<>(), new HashMap<>());

        var result = aggregator.buildGraph();

        assertThat(result.nodeCount()).isEqualTo(2);
        assertThat(result.relationshipCount()).isEqualTo(1);

        var graphStoreWithConfig = GraphStoreCatalog.get(userName, databaseId, graphName);
        var graphStore = graphStoreWithConfig.graphStore();

        assertThat(graphStore.nodes().toOriginalNodeId(0)).isEqualTo(source);
        assertThat(graphStore.nodes().toOriginalNodeId(1)).isEqualTo(target);
    }
}
