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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class LocalClusteringCoefficientMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A { name: 'a', seed: 2 })-[:T]->(b:A { name: 'b', seed: 2 }), " +
                                           "(b)-[:T]->(c:A { name: 'c', seed: 1 }), " +
                                           "(c)-[:T]->(a), " +
                                           "(a)-[:T]->(d:A { name: 'd', seed: 2 }), " +
                                           "(b)-[:T]->(d), " +
                                           "(c)-[:T]->(d), " +
                                           "(a)-[:T]->(e:A { name: 'e', seed: 2 }), " +
                                           "(b)-[:T]->(e) ";

    String expectedMutatedGraph = formatWithLocale(
        "  (a:A { lcc: %f })" +
        ", (b:A { lcc: %f })" +
        ", (c:A { lcc: %f })" +
        ", (d:A { lcc: %f })" +
        ", (e:A { lcc: %f })" +
        // Graph is UNDIRECTED, e.g. each rel twice
        ", (a)-[:T]->(b)" +
        ", (b)-[:T]->(a)" +
        ", (b)-[:T]->(c)" +
        ", (c)-[:T]->(b)" +
        ", (a)-[:T]->(c)" +
        ", (c)-[:T]->(a)" +

        ", (a)-[:T]->(d)" +
        ", (d)-[:T]->(a)" +
        ", (b)-[:T]->(d)" +
        ", (d)-[:T]->(b)" +

        ", (a)-[:T]->(e)" +
        ", (e)-[:T]->(a)" +
        ", (b)-[:T]->(e)" +
        ", (e)-[:T]->(b)" +

        ", (c)-[:T]->(d)" +
        ", (d)-[:T]->(c)",
        2.0 / 3, 2.0 / 3, 1.0, 1.0, 1.0
    );


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            LocalClusteringCoefficientMutateProc.class
        );

    }


    @Test
    void testMutateYields() {

        runQuery("CALL gds.graph.project('graph', {A: {label: 'A'}}, {T: {orientation: 'UNDIRECTED'}})");

        var query = "CALL gds.localClusteringCoefficient.mutate('graph', {mutateProperty: 'lcc'})";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("averageClusteringCoefficient"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(13.0 / 15.0, Offset.offset(1e-10));

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);

        });

        assertThat(rowCount).isEqualTo(1);

        Graph mutatedGraph = GraphStoreCatalog
            .get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore().getGraph(RelationshipType.of("T"));

        assertGraphEquals(fromGdl(expectedMutatedGraph), mutatedGraph);

    }

    @Test
    void testMutateSeeded() {
        runQuery(
            "CALL gds.graph.project('graph', {A: {label: 'A', properties: 'seed'}}, {T: {orientation: 'UNDIRECTED'}})");
        var query = "CALL gds.localClusteringCoefficient.mutate('graph', {mutateProperty: 'lcc', triangleCountProperty: 'seed'})";


        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("averageClusteringCoefficient"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(11.0 / 15.0, Offset.offset(1e-10));

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(5L);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);
        });

        assertThat(rowCount).isEqualTo(1);
    }
}


