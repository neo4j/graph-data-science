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
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThatCode;

class KnnFilteredGraphTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            KnnMutateProc.class,
            GraphStreamRelationshipPropertiesProc.class
        );
    }

    @Neo4jGraph
    static String cypher =
        "CREATE" +
        "  (peter:Person {knn: 3.0})" +
        ", (paul:Person {knn: 3.0})" +
        ", (secretariat:Horse {knn: 2.0})" +
        ", (ina_scott:Horse {knn: 3.0})" +
        ", (mary:Person {knn: 3.0})" +
        ", (:Person {knn: 3.0})" +
        ", (lexington:Horse {knn: 3.0})" +
        ", (:Person {knn: 3.0})" +
        ", (peter)-[:LIKES]->(mary)" +
        ", (paul)-[:KNOWS]->(peter)" +
        ", (secretariat)-[:LIKES]->(ina_scott)" +
        ", (lexington)-[:KNOWS]->(secretariat)";


    @Test
    void shouldComputeAndMutateOnFilteredGraph() {
        var graphProjectQuery = "CALL gds.graph.project(" +
                     "'g'," +
                     "['Person', 'Horse']," +
                     "['LIKES', 'KNOWS']" +
                     ", {" +
                     "nodeProperties: ['knn']" +
                     "}" +
                     ")";
        runQuery(graphProjectQuery);
        var knnQuery1 = "CALL gds.knn.mutate('g', {" +
                        "    nodeLabels: ['Horse']," +
                        "    mutateRelationshipType: 'FOO1'," +
                        "    mutateProperty: 'score'," +
                        "    nodeProperties: ['knn']" +
                        "})";
        var knnQuery2 = "CALL gds.knn.mutate('g', {" +
                        "    nodeLabels: ['Person']," +
                        "    mutateRelationshipType: 'FOO2'," +
                        "    mutateProperty: 'score'," +
                        "    nodeProperties: ['knn']" +
                        "})";
        assertThatCode(() -> runQuery(knnQuery1)).doesNotThrowAnyException();
        assertThatCode(() -> runQuery(knnQuery2)).doesNotThrowAnyException();
    }
}
