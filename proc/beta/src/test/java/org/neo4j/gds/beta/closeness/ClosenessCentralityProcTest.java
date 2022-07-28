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
package org.neo4j.gds.beta.closeness;

import org.assertj.core.api.SoftAssertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;

@Neo4jGraphExtension
abstract class ClosenessCentralityProcTest<CONFIG extends ClosenessCentralityConfig> extends BaseProcTest implements AlgoBaseProcTest<ClosenessCentrality, CONFIG, ClosenessCentralityResult> {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (n0:Node)" +
        ", (n1:Node)" +
        ", (n2:Node)" +
        ", (n3:Node)" +
        ", (n4:Node)" +
        ", (n5:Node)" +
        ", (n6:Node)" +
        ", (n7:Node)" +
        ", (n8:Node)" +
        ", (n9:Node)" +
        ", (n10:Node)" +

        // first ring
        ", (n1)-[:TYPE]->(n2)" +
        ", (n2)-[:TYPE]->(n3)" +
        ", (n3)-[:TYPE]->(n4)" +
        ", (n4)-[:TYPE]->(n5)" +
        ", (n5)-[:TYPE]->(n1)" +

        ", (n0)-[:TYPE]->(n0)" +
        ", (n1)-[:TYPE]->(n0)" +
        ", (n2)-[:TYPE]->(n0)" +
        ", (n3)-[:TYPE]->(n0)" +
        ", (n4)-[:TYPE]->(n0)" +
        ", (n5)-[:TYPE]->(n0)" +

        // second ring
        ", (n6)-[:TYPE]->(n7)" +
        ", (n7)-[:TYPE]->(n8)" +
        ", (n8)-[:TYPE]->(n9)" +
        ", (n9)-[:TYPE]->(n10)" +
        ", (n10)-[:TYPE]->(n6)" +

        ", (n0)-[:TYPE]->(n0)" +
        ", (n0)-[:TYPE]->(n1)" +
        ", (n0)-[:TYPE]->(n2)" +
        ", (n0)-[:TYPE]->(n3)" +
        ", (n0)-[:TYPE]->(n4)" +
        ", (n0)-[:TYPE]->(n5)" +
        ", (n0)-[:TYPE]->(n6)" +
        ", (n0)-[:TYPE]->(n7)" +
        ", (n0)-[:TYPE]->(n8)" +
        ", (n0)-[:TYPE]->(n9)" +
        ", (n0)-[:TYPE]->(n10)";

    @Inject
    private IdFunction idFunction;

    List<Map<String, Object>> expectedCentralityResult;

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(ClosenessCentralityResult result1, ClosenessCentralityResult result2) {
        var assertions = new SoftAssertions();

        var left = result1.centralities();
        var right = result2.centralities();

        assertions.assertThat(left.size()).as("Result size").isEqualTo(right.size());

        for (long i = 0; i < left.size(); i++) {
            assertions.assertThat(left.get(i)).as("Value at index " + i).isEqualTo(right.get(i));
        }

        assertions.assertAll();
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class
        );

        expectedCentralityResult = List.of(
            Map.of("nodeId", idFunction.of("n0"), "score", Matchers.closeTo(1.0, 0.01)),
            Map.of("nodeId", idFunction.of("n1"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n2"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n3"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n4"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n5"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n6"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n7"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n8"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n9"), "score", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n10"), "score", Matchers.closeTo(0.588, 0.01))
        );
    }
}
