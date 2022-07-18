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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class FilteredKnnIdMappingTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a {name: 'a', knn: 1.2})" +
        ", (b {name: 'b', knn: 1.1})" +
        ", (c {name: 'c', knn: 2.1})" +
        ", (d {name: 'd', knn: 3.1})" +
        ", (e {name: 'e', knn: 4.1})";

    @Test
    void shouldIdMapTheSourceNodeFilter() {
        // Offset the Neo ID space, then get the lowest Neo ID to use for sourceNodeFilter
        runQuery("UNWIND range(0, 10) AS foo CREATE ()");
        runQuery("MATCH (n) DELETE n");
        runQuery(DB_CYPHER);
        var lowestNeoId = runQuery("MATCH (n) RETURN id(n) AS id ORDER BY id ASC LIMIT 1", (r) -> (Long) r.next().get("id"));

        var graph = new StoreLoaderBuilder()
            .databaseService(db)
            .nodeProperties(List.of(PropertyMapping.of("knn")))
            .build()
            .graphStore()
            .getUnion();

        var config = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .topK(3)
            .randomJoins(0)
            .maxIterations(1)
            .randomSeed(20L)
            .concurrency(1)
            .sourceNodeFilter(NodeFilterSpecFactory.create(lowestNeoId))
            .build();
        var knn = FilteredKnn.createWithoutSeeding(graph, config, KnnContext.empty());

        var result = knn.compute();

        // filtering on the lowest Neo ID means all resulting similarity relationships have source node 0
        var sourceNodesInResult = result
            .similarityResultStream()
            .map(res -> res.node1)
            .collect(Collectors.<Long>toSet());
        assertThat(sourceNodesInResult).containsExactly(0L);
    }
}
