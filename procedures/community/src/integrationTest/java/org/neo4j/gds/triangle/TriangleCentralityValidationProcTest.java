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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TriangleCentralityValidationProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE (a:A)-[:T]->(b:B), (b)-[:T]->(c:A), (c)-[:T]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCentralityStreamProc.class,
            TriangleCentralityStatsProc.class,
            TriangleCentralityMutateProc.class,
            TriangleCentralityWriteProc.class
        );
        runQuery("CALL gds.graph.project('graph', ['A', 'B'], {T: {orientation: 'UNDIRECTED'}})");
    }

    @ParameterizedTest
    @ValueSource(strings = {"stream", "stats", "mutate", "write"})
    void shouldRejectDirectedGraphs(String mode) {
        runQuery("CALL gds.graph.project('directed', ['A', 'B'], 'T')");

        assertThatThrownBy(() -> execute(mode, "directed", configurationForMode(mode)))
            .hasMessageContaining("not all undirected");
    }

    @ParameterizedTest
    @ValueSource(strings = {"stream", "stats", "mutate", "write"})
    void shouldRejectUnknownLabels(String mode) {
        var config = configurationForMode(mode);
        config.put("labelFilter", List.of("X"));

        assertThatThrownBy(() -> execute(mode, "graph", config))
            .hasMessageContaining("TriangleCentrality requires the provided 'labelFilter' node label 'X'");
    }

    @ParameterizedTest
    @ValueSource(strings = {"stream", "stats", "mutate", "write"})
    void shouldRejectLabelsOutsideSelectedNodes(String mode) {
        var config = configurationForMode(mode);
        config.put("nodeLabels", List.of("A"));
        config.put("labelFilter", List.of("B"));

        // B exists in the projection, but triangles cannot use nodes excluded by nodeLabels.
        assertThatThrownBy(() -> execute(mode, "graph", config))
            .hasMessageContaining("TriangleCentrality requires the provided 'labelFilter' node label 'B'");
    }

    private void execute(String mode, String graph, Map<String, Object> config) {
        runQuery("CALL gds.triangleCentrality." + mode + "($graph, $config)", Map.of("graph", graph, "config", config));
    }

    private static Map<String, Object> configurationForMode(String mode) {
        var config = new HashMap<String, Object>();
        if (mode.equals("mutate")) {
            config.put("mutateProperty", "centrality");
        } else if (mode.equals("write")) {
            config.put("writeProperty", "centrality");
        }
        return config;
    }
}
