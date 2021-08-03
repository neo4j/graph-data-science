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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.graphalgo.AbstractRelationshipProjections;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.ConfigurableSeedConfigTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.HeapControlTest;
import org.neo4j.graphalgo.MemoryEstimateTest;
import org.neo4j.graphalgo.OnlyUndirectedTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_UNDIRECTED_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

abstract class LocalClusteringCoefficientBaseProcTest<CONFIG extends LocalClusteringCoefficientBaseConfig> extends BaseProcTest
    implements AlgoBaseProcTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    OnlyUndirectedTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    ConfigurableSeedConfigTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    MemoryEstimateTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result>,
    HeapControlTest<LocalClusteringCoefficient, CONFIG, LocalClusteringCoefficient.Result> {

    protected static final String TEST_GRAPH_NAME = "g";

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

    final Map<String, Double> expectedResult = Map.of(
        "a", 2.0 / 3,
        "b", 2.0 / 3,
        "c", 1.0,
        "d", 1.0,
        "e", 1.0
    );

    final Map<String, Double> expectedResultWithSeeding = Map.of(
        "a", 1.0 / 3,
        "b", 1.0 / 3,
        "c", 1.0 / 3,
        "d", 2.0 / 3,
        "e", 2.0
    );

    double expectedAverageClusteringCoefficientSeeded() {
        return expectedResultWithSeeding.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    double expectedAverageClusteringCoefficient() {
        return expectedResult.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class,
            getProcedureClazz()
        );

        runQuery(
            formatWithLocale(
                "CALL gds.graph.create('%s', {A: {label: 'A', properties: 'seed'}}, {T: {orientation: 'UNDIRECTED'}})",
                TEST_GRAPH_NAME
            )
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }


    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(
        LocalClusteringCoefficient.Result result1, LocalClusteringCoefficient.Result result2
    ) {

    }

    @Override
    public CypherMapWrapper createMinimalImplicitConfig(CypherMapWrapper mapWrapper) {
        if (mapWrapper.containsKey(RELATIONSHIP_PROJECTION_KEY) || mapWrapper.containsKey(RELATIONSHIP_QUERY_KEY)) {
            return createMinimalConfig(CypherMapWrapper.create(anonymousGraphConfig(mapWrapper.toMap())));
        }

        return createMinimalConfig(CypherMapWrapper.create(anonymousGraphConfig(mapWrapper
            .withEntry(RELATIONSHIP_PROJECTION_KEY, relationshipProjections())
            .toMap())));
    }

    @Override
    public RelationshipProjections relationshipProjections() {
        return AbstractRelationshipProjections.ALL_UNDIRECTED;
    }

    @Override
    public String relationshipQuery() {
        return ALL_RELATIONSHIPS_UNDIRECTED_QUERY;
    }

    @Override
    public String seedPropertyKeyOverride() {
        return "triangleCountProperty";
    }

    @Test
    void warnOnNoneAggregatedGraph() {
        var graphName = "nonagg";
        runQuery("CALL gds.graph.create(" +
                 " $graphName," +
                 " '*', {" +
                 "    ALL: {" +
                 "        type: '*'," +
                 "        orientation: 'UNDIRECTED'" +
                 "    }" +
                 " })", Map.of("graphName", graphName));

        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.empty());

        var testLog = new TestLog();

        applyOnProcedure(proc -> {
            proc.log = testLog;
            getProcedureMethods(proc)
                .filter(procMethod -> !getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(noneEstimateMethod -> {
                    try {
                        noneEstimateMethod.invoke(proc, "nonagg", config.toMap());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
        });

        String expected = "Procedure runs optimal with relationship aggregation. " +
                          "Projection for `ALL` does not aggregate relationships. " +
                          "You might experience a slowdown in the procedure execution.";
        String actual = testLog.getMessages("warn").get(0);
        assertEquals(expected, actual);
    }

    @Override
    public void loadGraph(String graphName){
        String graphCreateQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .graphCreate(graphName)
            .yields();

        runQuery(graphCreateQuery);
    }
}
