/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.pagerank;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.ImmutablePropertyMapping;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.ToleranceConfigTest;
import org.neo4j.graphalgo.RelationshipWeightConfigTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.IterationsConfigTest;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

abstract class PageRankBaseProcTest<CONFIG extends PageRankBaseConfig> extends BaseProcTest implements
    AlgoBaseProcTest<CONFIG, PageRank>,
    IterationsConfigTest<CONFIG, PageRank>,
    RelationshipWeightConfigTest<CONFIG, PageRank>,
    ToleranceConfigTest<CONFIG, PageRank>,
    MemoryEstimateTest<CONFIG, PageRank> {

    static Map<Long, Double> expected = new HashMap<>();
    static Map<Long, Double> weightedExpected = new HashMap<>();

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        @Language("Cypher") String cypher =
            "CREATE" +
            "  (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +
            ", (u:Label3 {name: 'u'})" +
            ", (v:Label3 {name: 'v'})" +
            ", (w:Label3 {name: 'w'})" +
            ", (b)-[:TYPE1 {weight: 1.0,  equalWeight: 1.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 1.2,  equalWeight: 1.0}]->(b)" +
            ", (d)-[:TYPE1 {weight: 1.3,  equalWeight: 1.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 1.7,  equalWeight: 1.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 6.1,  equalWeight: 1.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.2,  equalWeight: 1.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 1.5,  equalWeight: 1.0}]->(f)" +
            ", (f)-[:TYPE1 {weight: 10.5, equalWeight: 1.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.9,  equalWeight: 1.0}]->(e)" +
            ", (g)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(b)" +
            ", (g)-[:TYPE2 {weight: 5.3,  equalWeight: 1.0}]->(e)" +
            ", (h)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(b)" +
            ", (h)-[:TYPE2 {weight: 0.3,  equalWeight: 1.0}]->(e)" +
            ", (i)-[:TYPE2 {weight: 5.4,  equalWeight: 1.0}]->(b)" +
            ", (i)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(e)" +
            ", (j)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(e)" +
            ", (k)-[:TYPE2 {weight: 4.2,  equalWeight: 1.0}]->(e)" +
            ", (u)-[:TYPE3 {weight: 1.0}]->(v)" +
            ", (u)-[:TYPE3 {weight: 1.0}]->(w)" +
            ", (v)-[:TYPE3 {weight: 1.0}]->(w)";

        registerProcedures(PageRankStreamProc.class, PageRankWriteProc.class, GraphCreateProc.class);
        runQuery(cypher);

        runQuery("CALL gds.graph.create(" +
                 "'graphLabel1'," +
                 "'Label1'," +
                 "   {" +
                 "      TYPE1: {" +
                 "          type: 'TYPE1'," +
                 "          properties: ['weight', 'equalWeight']" +
                 "      } " +
                 "   }" +
                 ")");

        runQuery("CALL gds.graph.create(" +
                 "'graphLabel3'," +
                 "'Label3'," +
                 "   {" +
                 "      TYPE3: {" +
                 "          type: 'TYPE3'," +
                 "          properties: ['equalWeight'], " +
                 "          projection: 'UNDIRECTED'" +
                 "      } " +
                 "   }" +
                 ")");

        runInTransaction(db, () -> {
            final Label label = Label.label("Label1");
            expected.put(db.findNode(label, "name", "a").getId(), 0.243);
            expected.put(db.findNode(label, "name", "b").getId(), 1.844);
            expected.put(db.findNode(label, "name", "c").getId(), 1.777);
            expected.put(db.findNode(label, "name", "d").getId(), 0.218);
            expected.put(db.findNode(label, "name", "e").getId(), 0.243);
            expected.put(db.findNode(label, "name", "f").getId(), 0.218);
            expected.put(db.findNode(label, "name", "g").getId(), 0.150);
            expected.put(db.findNode(label, "name", "h").getId(), 0.150);
            expected.put(db.findNode(label, "name", "i").getId(), 0.150);
            expected.put(db.findNode(label, "name", "j").getId(), 0.150);

            weightedExpected.put(db.findNode(label, "name", "a").getId(), 0.218);
            weightedExpected.put(db.findNode(label, "name", "b").getId(), 2.008);
            weightedExpected.put(db.findNode(label, "name", "c").getId(), 1.850);
            weightedExpected.put(db.findNode(label, "name", "d").getId(), 0.185);
            weightedExpected.put(db.findNode(label, "name", "e").getId(), 0.182);
            weightedExpected.put(db.findNode(label, "name", "f").getId(), 0.174);
            weightedExpected.put(db.findNode(label, "name", "g").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "h").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "i").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "j").getId(), 0.150);
        });
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    static Stream<Arguments> graphVariations() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType("TYPE1")
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsWeight() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType(
                        "TYPE1",
                        RelationshipProjection.builder()
                            .type("TYPE1")
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("weight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsEqualWeight() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel1").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label1")
                    .withRelationshipType(
                        "TYPE1",
                        RelationshipProjection.builder()
                            .type("TYPE1")
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("equalWeight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    static Stream<Arguments> graphVariationsLabel3() {
        return Stream.of(
            arguments(
                GdsCypher.call().explicitCreation("graphLabel3").algo("pageRank"),
                "explicit graph"
            ),
            arguments(
                GdsCypher.call()
                    .withNodeLabel("Label3")
                    .withRelationshipType(
                        "TYPE3",
                        RelationshipProjection.builder()
                            .type("TYPE3")
                            .orientation(Orientation.UNDIRECTED)
                            .addProperty(ImmutablePropertyMapping.builder().propertyKey("equalWeight").build())
                            .build()
                    )
                    .algo("pageRank"),
                "implicit graph"
            )
        );
    }

    @Override
    public void assertResultEquals(PageRank result1, PageRank result2) {
        HugeDoubleArray resultArray1 = result1.result().array();
        HugeDoubleArray resultArray2 = result2.result().array();
        assertArrayEquals(resultArray1.toArray(), resultArray2.toArray());
    }

    @Test
    void testDampingFactorFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("dampingFactor", 0.85));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals(0.85, config.dampingFactor());
    }
}
