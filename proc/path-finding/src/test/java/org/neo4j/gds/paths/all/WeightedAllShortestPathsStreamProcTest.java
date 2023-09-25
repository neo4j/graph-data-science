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
package org.neo4j.gds.paths.all;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**         5     5      5
 *      (1)---(2)---(3)----.
 *    5/ 2    2     2     2 \     5
 *  (0)---(7)---(8)---(9)---(10)-//->(0)
 *    3\    3     3     3   /
 *      (4)---(5)---(6)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
final class WeightedAllShortestPathsStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
            "CREATE" +
            "  (s:Node {name: 's'})" +
            ", (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +
            ", (x:Node {name: 'x'})" +

            ", (x)-[:TYPE {cost: 5}]->(s)" + // creates cycle

            ", (s)-[:TYPE {cost: 5}]->(a)" + // line 1
            ", (a)-[:TYPE {cost: 5}]->(b)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (c)-[:TYPE {cost: 5}]->(x)" +

            ", (s)-[:TYPE {cost: 3}]->(d)" + // line 2
            ", (d)-[:TYPE {cost: 3}]->(e)" +
            ", (e)-[:TYPE {cost: 3}]->(f)" +
            ", (f)-[:TYPE {cost: 3}]->(x)" +

            ", (s)-[:TYPE {cost: 2}]->(g)" + // line 3
            ", (g)-[:TYPE {cost: 2}]->(h)" +
            ", (h)-[:TYPE {cost: 2}]->(i)" +
            ", (i)-[:TYPE {cost: 2}]->(x)";

    private static long startNodeId;
    private static long targetNodeId;

    @Inject
    IdFunction idFunction;


    @BeforeEach
    void setup() throws Exception {

        registerProcedures(AllShortestPathsStreamProc.class, GraphProjectProc.class);

        startNodeId = idFunction.of("s");
        targetNodeId = idFunction.of("x");
    }

    @Test
    void testMSBFSASP() {

        loadCompleteGraph(DEFAULT_GRAPH_NAME);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.allShortestPaths")
            .streamMode()
            .yields();

        MutableDouble wantedDistance = new MutableDouble();
        LongAdder timesAppeared = new LongAdder();
        var rowCount = runQueryWithRowConsumer(query, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertThat(distance).isNotEqualTo(Double.POSITIVE_INFINITY);
            if (source == target) {
                assertThat(distance).isCloseTo(0, Offset.offset(1e-3));
            }
            if (source == startNodeId && target == targetNodeId) {
                timesAppeared.increment();
                wantedDistance.setValue(distance);
            }
        });

        //11 nodes: 110 pairs
        assertThat(rowCount).isEqualTo(110);
        // 4 steps from start to end max
        assertThat(timesAppeared.longValue()).isEqualTo(1l);
        assertThat(wantedDistance.doubleValue()).isEqualTo(4.0);
    }

    @Test
    void testMSBFSASPIncoming() {

        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.REVERSE);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.allShortestPaths")
            .streamMode()
            .yields();

        MutableDouble wantedDistance = new MutableDouble();
        LongAdder timesAppeared = new LongAdder();
        var rowCount = runQueryWithRowConsumer(query, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertThat(distance).isNotEqualTo(Double.POSITIVE_INFINITY);
            if (source == target) {
                assertThat(distance).isCloseTo(0, Offset.offset(1e-3));
            }
            if (source == targetNodeId && target == startNodeId) {
                timesAppeared.increment();
                wantedDistance.setValue(distance);
            }
        });

        //11 nodes: 110 pairs
        assertThat(rowCount).isEqualTo(110);
        // 4 steps from start to end max
        assertThat(timesAppeared.longValue()).isEqualTo(1l);
        assertThat(wantedDistance.doubleValue()).isEqualTo(4.0);

    }

    @Test
    void testWeightedASP() {

        var createQuery = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("cost")
            .loadEverything()
            .yields();
        runQuery(createQuery);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.allShortestPaths")
            .streamMode()
            .addParameter("relationshipWeightProperty", "cost")
            .yields();

        MutableDouble wantedDistance = new MutableDouble();
        LongAdder timesAppeared = new LongAdder();
        var rowCount = runQueryWithRowConsumer(query, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertThat(distance).isNotEqualTo(Double.POSITIVE_INFINITY);
            if (source == target) {
                assertThat(distance).isCloseTo(0, Offset.offset(1e-3));
            }
            if (source == startNodeId && target == targetNodeId) {
                timesAppeared.increment();
                wantedDistance.setValue(distance);
            }
        });

        //11 nodes: 110 pairs + 11 paths to itself
        assertThat(rowCount).isEqualTo(121);
        // 4 steps from start to end max
        assertThat(timesAppeared.longValue()).isEqualTo(1l);
        assertThat(wantedDistance.doubleValue()).isEqualTo(8.0);

    }

}
