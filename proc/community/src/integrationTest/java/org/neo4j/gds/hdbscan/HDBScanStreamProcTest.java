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
package org.neo4j.gds.hdbscan;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class HDBScanStreamProcTest extends BaseProcTest {

    @Inject
    private IdFunction idFunction;

    @Neo4jGraph
    @Language("Cypher")
    static final String DB_CYPHER =
        """
            CREATE
                (a:Node {point: [1.17755754, 2.02742572]}),
                (b:Node {point: [0.88489682, 1.97328227]}),
                (c:Node {point: [1.04192267, 4.34997048]}),
                (d:Node {point: [1.25764886, 1.94667762]}),
                (e:Node {point: [0.95464318, 1.55300632]}),
                (f:Node {point: [0.80617459, 1.60491802]}),
                (g:Node {point: [1.26227786, 3.96066446]}),
                (h:Node {point: [0.87569985, 4.51938412]}),
                (i:Node {point: [0.8028515 , 4.088106  ]}),
                (j:Node {point: [0.82954022, 4.63897487]})
            """;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            HDBScanStreamProc.class
        );

        runQuery(
            """
                    MATCH (n:Node)
                    WITH gds.graph.project(
                      'points',
                      n,
                      null,
                      {
                        sourceNodeProperties: {point: n.point},
                        targetNodeProperties: null
                      }
                    ) as g
                    RETURN g.graphName AS graph, g.nodeCount AS nodes
                """
        );
    }

    @Test
    void stream() {

        runQuery(
            """
                    CALL gds.hdbscan.stream(
                        'points',
                        {
                            nodeProperty: 'point',
                            leafSize: 1,
                            samples: 2,
                            minClusterSize: 2 
                        }
                    )
                """,
            result -> {
                assertThat(result.columns()).containsExactlyInAnyOrder("nodeId", "label");
                long resultRowCount = 0;
                var communities = new HashSet<Long>();
                while (result.hasNext()) {
                    var next = result.next();
                    assertThat(next.get("nodeId")).isInstanceOf(Long.class);
                    assertThat(next.get("label")).isInstanceOf(Long.class);
                    communities.add((long) next.get("label"));
                    resultRowCount++;
                }
                assertThat(resultRowCount).isEqualTo(10L);
                assertThat(communities).hasSize(2);
                return true;
            }
        );

    }
}
