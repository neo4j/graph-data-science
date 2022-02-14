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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PageRankStreamProcTest extends PageRankProcTest<PageRankStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<PageRankAlgorithm, PageRankResult, PageRankStreamConfig, ?>> getProcedureClazz() {
        return PageRankStreamProc.class;
    }

    @Override
    public PageRankStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankStreamConfig.of(mapWrapper);
    }

    @Test
    void testWeightedPageRankFromLoadedGraphWithDirectionBoth() {
        String query = GdsCypher.call("graphLabel1")
            .algo("pageRank")
            .streamMode()
            .addParameter("relationshipWeightProperty", "equalWeight")
            .addParameter("maxIterations", 1)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query,
            row -> actual.put(row.getNumber("nodeId").longValue(), row.getNumber("score").doubleValue()));

        long distinctValues = actual.values().stream().distinct().count();
        assertEquals(1, distinctValues);
    }

    @Test
    void testWeightedPageRankThrowsIfWeightPropertyDoesNotExist() {
        String query = GdsCypher.call("graphLabel1")
            .algo("pageRank")
            .streamMode()
            .addParameter("relationshipWeightProperty", "does_not_exist")
            .yields("nodeId", "score");

        assertThatThrownBy(() -> {
            runQueryWithRowConsumer(query, row -> {});
        }).isInstanceOf(QueryExecutionException.class)
            .hasMessageContaining("Relationship weight property `does_not_exist` not found in relationship types ['TYPE1']")
            .hasMessageContaining("Properties existing on all relationship types: ['equalWeight', 'weight']");
    }

    @Test
    void testPageRank() {
        String query = GdsCypher.call("graphLabel1")
            .algo("pageRank")
            .streamMode()
            .yields("nodeId", "score");

        assertCypherResult(query, expected);
    }

    @Test
    void testWeightedPageRank() {
        String query = GdsCypher.call("graphLabel1")
            .algo("pageRank")
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .yields("nodeId", "score");

        assertCypherResult(query, weightedExpected);
    }

    @Test
    void testCacheWeightsIsUnknown() {
        loadGraph(DEFAULT_GRAPH_NAME);
        var config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            "tolerance", 3.14,
            "cacheWeights", true
        )));

        var log = Neo4jProxy.testLog();

        assertThatThrownBy(() -> {
            applyOnProcedure(proc -> {
                ((PageRankStreamProc) proc).stream(DEFAULT_GRAPH_NAME, config.toMap());
            });
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unexpected configuration key: cacheWeights");
    }

    @Test
    void streamWithSourceNodes() {
        var sourceNodes = allNodesWithLabel("Label1");

        String queryWithSourceNodes = GdsCypher.call("graphLabel1")
            .algo("pageRank")
            .streamMode()
            .addPlaceholder("sourceNodes", "sources")
            .yields();

        assertCypherResult(queryWithSourceNodes, Map.of("sources", sourceNodes), expected);
    }

}
