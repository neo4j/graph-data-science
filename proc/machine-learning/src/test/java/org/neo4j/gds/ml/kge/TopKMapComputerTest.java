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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class TopKMapComputerTest extends BaseTest {


    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:N {emb: [0.1, 0.2, 0.3]})" +
            ", (b:N {emb: [0.1, 0.2, 0.3]})" +
            ", (c:N {emb: [0.1, 0.2, 0.3]})" +
            ", (d:M {emb: [0.1, 0.2, 0.3]})" +
            ", (e:M {emb: [0.1, 0.2, 0.3]})" +
            ", (f:M {emb: [0.1, 0.2, 0.3]})" +
            ", (a)-[:REL {prop: 1.0}]->(b)" +
            ", (b)-[:REL {prop: 1.0}]->(a)" +
            ", (a)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(a)" +
            ", (b)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(b)";

    @BeforeEach
    void setUp() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldComputeTopKMap() {
        Graph graph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty("emb", "emb", DefaultValue.of(new double[]{0.0, 0.0, 0.0}), Aggregation.NONE)
            .build()
            .graph();

        var sourceNodes = create(0, 1, 2);
        var targetNodes = create(3, 4, 5);
        var topK = 1;
        var concurrency = 1;

        var computer = new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            "emb",
            List.of(0.1, 0.2, 0.3),
            "DistMult",
            (a, b) -> a != b,
            topK,
            concurrency,
            ProgressTracker.NULL_TRACKER
        );

        KGEPredictResult result = computer.compute();

        var resultSourceNodes = result.topKMap()
            .stream()
            .map(SimilarityResult::sourceNodeId)
            .collect(Collectors.toList());
        assertThat(resultSourceNodes).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    private BitSet create(long... ids) {
        long capacity = Arrays.stream(ids).max().orElse(0);

        BitSet bitSet = new BitSet(capacity + 1);

        for (long id : ids) {
            bitSet.set(id);
        }

        return bitSet;
    }
}
