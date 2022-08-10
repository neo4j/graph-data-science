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
package org.neo4j.gds.betweenness;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WeightedBetweennessStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String weightedGdl =
        "CREATE " +
        "  (a1: Node)" +
        ", (b: Node)" +
        ", (a2: Node)" +
        ", (c: Node)" +
        ", (d: Node)" +
        ", (e: Node)" +
        ", (f: Node)" +
        ", (a1)-[:REL {weight: 1.0}]->(b)" +
        ", (a2)-[:REL {weight: 1.0}]->(b)" +
        ", (b) -[:REL {weight: 1.0}]->(c)" +
        ", (b) -[:REL {weight: 1.3}]->(d)" +
        ", (c) -[:REL {weight: 1.0}]->(e)" +
        ", (d) -[:REL {weight: 0.2}]->(e)" +
        ", (e) -[:REL {weight: 1.0}]->(f)";


    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            BetweennessCentralityStreamProc.class
        );

        var loadQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType(
                "REL",
                RelationshipProjection.of(
                    "REL",
                    Orientation.NATURAL,
                    Aggregation.DEFAULT
                )
            )
            .withRelationshipProperty("weight")
            .yields();

        runQuery(loadQuery);
    }

    @Test
    void shouldWorkWithWeights() {
        var expected = Map.of(
            idFunction.of("a1"), 0.0D,
            idFunction.of("a2"), 0.0D,
            idFunction.of("b"), 8.0D,
            idFunction.of("c"), 0.0D,
            idFunction.of("d"), 6.0D,
            idFunction.of("e"), 5.0D,
            idFunction.of("f"), 0.0D
        );

        var bcQuery = "CALL gds.betweenness.stream('graph',{relationshipWeightProperty: 'weight'})";
        var resultRowCount = new MutableLong();
        var scores = new HashMap<Long, Double>();

        runQueryWithRowConsumer(bcQuery, resultRow -> {
            var nodeId = resultRow.getNumber("nodeId");
            var score = resultRow.getNumber("score");
            assertThat(nodeId).isInstanceOf(Long.class);
            assertThat(score).isInstanceOf(Double.class);
            resultRowCount.increment();
            scores.put(nodeId.longValue(), score.doubleValue());

        });
        assertThat(resultRowCount.longValue()).isEqualTo(7L);
        assertThat(scores).containsExactlyInAnyOrderEntriesOf(expected);
    }
}
