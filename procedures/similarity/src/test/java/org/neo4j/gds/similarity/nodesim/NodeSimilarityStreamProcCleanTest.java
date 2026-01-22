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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThatNoException;

class NodeSimilarityStreamProcCleanTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
            "  (a:Person {id: 0,  name: 'Alice'})" +
            ", (b:Person {id: 1,  name: 'Bob'})" +
            ", (c:Person {id: 2,  name: 'Charlie'})" +
            ", (d:Person {id: 3,  name: 'Dave'})" +
            ", (i1:Item  {id: 10, name: 'p1'})" +
            ", (i2:Item  {id: 11, name: 'p2'})" +
            ", (i3:Item  {id: 12, name: 'p3'})" +
            ", (i4:Item  {id: 13, name: 'p4'})" +
            ", (a)-[:LIKES]->(i1)" +
            ", (a)-[:LIKES]->(i2)" +
            ", (a)-[:LIKES]->(i3)" +
            ", (b)-[:LIKES]->(i1)" +
            ", (b)-[:LIKES]->(i2)" +
            ", (c)-[:LIKES]->(i3)";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            NodeSimilarityStreamProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project('g', '*', 'LIKES')");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldWorkWithCosineSimilarityMetric() {
        assertThatNoException().isThrownBy(
            () -> runQuery("CALL gds.nodeSimilarity.stream('g', {similarityCutoff: 0.0, similarityMetric: 'cosine'})")
        );
    }
}
