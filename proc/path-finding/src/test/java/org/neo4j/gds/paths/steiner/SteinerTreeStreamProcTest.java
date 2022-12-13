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
package org.neo4j.gds.paths.steiner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class SteinerTreeStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE(a:Node) " +
                                    "CREATE(b:Node) " +
                                    "CREATE(c:Node) " +
                                    "CREATE (a)-[:TYPE {cost:1.0}]->(b) ";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SteinerTreeStreamProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("TYPE", Orientation.NATURAL)
            .withRelationshipProperty("cost")
            .yields();
        runQuery(createQuery);
    }


    private long getSourceNode() {
        return idFunction.of("a");
    }

    private long getTerminal() {
        return idFunction.of("b");
    }

    private long getUnreachableTerminal() {
        return idFunction.of("c");
    }

    @Test
    void testYields() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.steinerTree")
            .streamMode()
            .addParameter("sourceNode", getSourceNode())
            .addParameter("targetNodes", List.of(getTerminal()))
            .addParameter("relationshipWeightProperty", "cost")
            .yields(
                "nodeId", "parentId", "weight"
            );
        LongAdder nodeCount = new LongAdder();

        runQuery(query, result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder("nodeId", "parentId", "weight");

            while (result.hasNext()) {
                var next = result.next();
                assertThat(next.get("nodeId")).isInstanceOf(Long.class);
                assertThat(next.get("parentId")).isInstanceOf(Long.class);
                assertThat(next.get("weight")).isInstanceOf(Double.class);
                assertThat((long) next.get("nodeId")).isIn(List.of(getSourceNode(),getTerminal()));
                assertThat((long) next.get("parentId")).isEqualTo(getSourceNode());
                assertThat((double) next.get("weight")).isIn(0.0, 1.0);
                nodeCount.increment();
            }

            return true;

        });
        assertThat(nodeCount.intValue()).isEqualTo(2);
    }
    

}
