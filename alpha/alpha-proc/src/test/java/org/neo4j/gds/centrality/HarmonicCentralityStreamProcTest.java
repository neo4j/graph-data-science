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
package org.neo4j.gds.centrality;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class HarmonicCentralityStreamProcTest  extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE (a:Node {name:'a'})" +
        ",      (b:Node {name:'b'})" +
        ",      (c:Node {name:'c'})" +
        ",      (d:Node {name:'d'})" +
        ",      (e:Node {name:'e'})" +
        ",      (a)-[:TYPE]->(b)" +
        ",      (b)-[:TYPE]->(c)" +
        ",      (d)-[:TYPE]->(e)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            HarmonicCentralityStreamProc.class,
            GraphProjectProc.class
        );
        loadCompleteGraph("graph", Orientation.UNDIRECTED);
    }

    @Test
    void shouldStream() {
        var query = GdsCypher.call("graph")
            .algo("gds.alpha.closeness.harmonic")
            .streamMode()
            .yields("nodeId", "centrality");

        var resultMap = new HashMap<Long, Double>();
        var rowCount=runQueryWithRowConsumer(query, row -> {
            resultMap.put(
                row.getNumber("nodeId").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        assertThat(rowCount).isEqualTo(5);

        assertThat( resultMap.get(idFunction.of("a")).doubleValue()).isCloseTo(0.375, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("b")).doubleValue()).isCloseTo(0.5, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("c")).doubleValue()).isCloseTo(0.375, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("d")).doubleValue()).isCloseTo(0.25, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("e")).doubleValue()).isCloseTo(0.25, Offset.offset(1e-5));
    }
}
