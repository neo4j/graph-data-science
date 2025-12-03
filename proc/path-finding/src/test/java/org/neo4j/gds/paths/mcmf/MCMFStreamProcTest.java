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
package org.neo4j.gds.paths.mcmf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class MCMFStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    static final String DB_CYPHER = """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0, c:2.0}]->(d),
                (b)-[:R {w: 3.0, c:2.0}]->(a),
                (c)-[:R {w: 2.0, c:2.0}]->(a),
                (c)-[:R {w: 0.0, c:2.0}]->(b),
                (d)-[:R {w: 5.0, c:2.0}]->(e)
            """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(MCMFStreamProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipProperty("w")
            .withRelationshipProperty("c")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testYields() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.maxFlow.minCost")
            .streamMode()
            .addParameter("sourceNodes", idFunction.of("a"))
            .addParameter("capacityProperty", "w")
            .addParameter("costProperty", "c")
            .addParameter("targetNodes", idFunction.of("e"))
            .yields(
                "source", "target", "flow"
            );
        LongAdder relCount = new LongAdder();

        var set = new HashSet<Arc>();
        runQuery(query, result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder("source", "target", "flow");

            while (result.hasNext()) {
                var next = result.next();
                assertThat(next.get("source")).isInstanceOf(Long.class);
                assertThat(next.get("target")).isInstanceOf(Long.class);
                assertThat(next.get("flow")).isInstanceOf(Double.class);

                set.add(new Arc((long)next.get("source"), (long)next.get("target"), (double)next.get("flow")));
                relCount.increment();
            }

            return true;

        });
        assertThat(relCount.intValue()).isEqualTo(2);
        assertThat(set).containsExactlyInAnyOrder(new Arc(idFunction.of("a"), idFunction.of("d"), 4.0D), new Arc(idFunction.of("d"), idFunction.of("e"), 4.0D));
    }

    private record Arc(long source, long target, double flow) {
    }
}
