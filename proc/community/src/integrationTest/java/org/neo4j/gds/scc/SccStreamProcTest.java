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
package org.neo4j.gds.scc;

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class SccStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +

            ", (a)-[:TYPE {cost: 5}]->(b)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (c)-[:TYPE {cost: 5}]->(a)" +

            ", (d)-[:TYPE {cost: 2}]->(e)" +
            ", (e)-[:TYPE {cost: 2}]->(f)" +
            ", (f)-[:TYPE {cost: 2}]->(d)" +

            ", (a)-[:TYPE {cost: 2}]->(d)" +

            ", (g)-[:TYPE {cost: 3}]->(h)" +
            ", (h)-[:TYPE {cost: 3}]->(i)" +
            ", (i)-[:TYPE {cost: 3}]->(g)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            SccStreamProc.class
        );

        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
    }

    @Test
    void shouldStream() {

        final IntIntScatterMap testMap = new IntIntScatterMap();

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.scc")
            .streamMode()
            .yields();

        var rowCount = runQueryWithRowConsumer(query, row ->
            testMap.put(row.getNumber("nodeId").intValue(), row.getNumber("componentId").intValue())
        );

        Function<String, Integer> community = node -> testMap.get((int) idFunction.of(node));

        assertThat(rowCount).isEqualTo(9l);

        assertThat(community.apply("a")).isEqualTo(community.apply("b")).isEqualTo(community.apply("c"));
        assertThat(community.apply("d")).isEqualTo(community.apply("e")).isEqualTo(community.apply("f"));
        assertThat(community.apply("g")).isEqualTo(community.apply("h")).isEqualTo(community.apply("i"));

        assertThat(community.apply("a")).isNotEqualTo(community.apply("d"));
        assertThat(community.apply("d")).isNotEqualTo(community.apply("g"));
        assertThat(community.apply("a")).isNotEqualTo(community.apply("g"));


        // 3 sets with 3 elements each

    }

    @Test
    void shouldStreamWithConsecutiveIds() {

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.scc")
            .streamMode()
            .addParameter("consecutiveIds", true)
            .yields();

        HashSet<Long> components = new HashSet<>();
        runQueryWithRowConsumer(query, row ->
            components.add(row.getNumber("componentId").longValue())
        );

        assertThat(components).containsExactly(0L, 1L, 2L);

    }
}
