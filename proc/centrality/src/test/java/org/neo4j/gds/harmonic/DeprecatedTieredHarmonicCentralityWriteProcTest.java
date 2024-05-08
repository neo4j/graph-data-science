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
package org.neo4j.gds.harmonic;

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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DeprecatedTieredHarmonicCentralityWriteProcTest extends BaseProcTest {

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

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            HarmonicCentralityWriteProc.class,
            GraphProjectProc.class
        );
        loadCompleteGraph("graph", Orientation.UNDIRECTED);
    }

    @Test
    void testWrite() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.alpha.closeness.harmonic")
            .writeMode()
            .yields();

        var rowCount= runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("nodes")).isEqualTo(5L);
            assertThat(row.getString("writeProperty")).isEqualTo("centrality");

            assertThat((long)row.getNumber("writeMillis")).isGreaterThan(-1L);
            assertThat((long)row.getNumber("computeMillis")).isGreaterThan(-1L);
            assertThat((long)row.getNumber("preProcessingMillis")).isGreaterThan(-1L);

            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertThat( (double)centralityDistribution.get("max")).isCloseTo(0.5,Offset.offset(1e-5));
        });

        assertThat(rowCount).isEqualTo(1);


        var resultQuery = "MATCH (n) RETURN id(n) AS id, n.centrality AS centrality";

        var resultMap = new HashMap<Long, Double>();
        runQueryWithRowConsumer(resultQuery, row -> {
            resultMap.put(
                row.getNumber("id").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });


        assertThat( resultMap.get(idFunction.of("a")).doubleValue()).isCloseTo(0.375, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("b")).doubleValue()).isCloseTo(0.5, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("c")).doubleValue()).isCloseTo(0.375, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("d")).doubleValue()).isCloseTo(0.25, Offset.offset(1e-5));
        assertThat( resultMap.get(idFunction.of("e")).doubleValue()).isCloseTo(0.25, Offset.offset(1e-5));
    }
}
