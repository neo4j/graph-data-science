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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.catalog.GraphWriteRelationshipProc;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromCypherConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.config.IterationsConfigProcTest;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.gds.utils.SimilarityHelper.assertSimilarityStreamsAreEqual;

abstract class KnnProcTest<CONFIG extends KnnBaseConfig> extends BaseProcTest implements
    MemoryEstimateTest<Knn, CONFIG, Knn.Result> {

    @TestFactory
    Stream<DynamicTest> configTests() {
        // KNN's createMinimalConfig() is not really minimal, but deterministic
        // it overrides several default parameters, and we don't want or need that
        // but we do need it to get the mutate and write mandatory params, so we'll just remove the default ones
        var minimalConfig = createMinimalConfig()
            .withoutEntry("randomSeed")
            .withoutEntry("concurrency")
            .withoutEntry("sampleRate")
            .withoutEntry("deltaThreshold")
            .withoutEntry("randomJoins")
            .withoutEntry("topK");

        return Stream.concat(
            modeSpecificConfigTests(),
            Stream.of(
                IterationsConfigProcTest.test(proc(), minimalConfig)
            ).flatMap(Collection::stream)
        );
    }

    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.empty();
    }

    static final String GRAPH_NAME = "myGraph";

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
                                       "  (a { id: 1, knn: 1.0 } )" +
                                       ", (b { id: 2, knn: 2.0 } )" +
                                       ", (c { id: 3, knn: 5.0 } )" +
                                       ", (a)-[:IGNORE]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphWriteRelationshipProc.class
        );

        String graphCreateQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("knn")
            .withRelationshipType("IGNORE")
            .yields();

        runQuery(graphCreateQuery);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(Knn.Result result1, Knn.Result result2) {
        assertSimilarityStreamsAreEqual(result1.streamSimilarityResult(), result2.streamSimilarityResult());
    }

    @Override
    public @NotNull GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        GraphProjectConfig configWithNodeProperty = graphProjectConfig instanceof GraphProjectFromStoreConfig
            ? ImmutableGraphProjectFromStoreConfig
                .builder()
                .from(graphProjectConfig)
                .nodeProperties(PropertyMappings.of(PropertyMapping.of("knn")))
                .build()
            : ImmutableGraphProjectFromCypherConfig
                .builder()
                .from(graphProjectConfig)
                .nodeQuery("MATCH (n) RETURN id(n) AS id, n.knn AS knn")
                .build();

        return graphLoader(graphDb(), configWithNodeProperty);
    }

    @Override
    public void loadGraph(String graphName) {
        QueryRunner.runQuery(
            graphDb(),
            GdsCypher.call(graphName)
                .graphProject()
                .withAnyLabel()
                .withNodeProperty("knn")
                .withAnyRelationshipType()
                .yields()
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        // In case we're not testing something explicit with concurrency we want a deterministic result.
        // For this reason we set concurrency to 1 and fix a randomSeed != -1.

        if (!mapWrapper.containsKey("concurrency")) {
            mapWrapper = mapWrapper.withNumber("concurrency", 1);
        }

        if (mapWrapper.getInt("concurrency", 1) == 1) {
            // example of a bad seed with randomJoins=10: 3382659597966568962L
            mapWrapper = mapWrapper.withNumber("randomSeed", 1337L);
        }

        return mapWrapper
            .withNumber("sampleRate", 1.0)
            .withNumber("deltaThreshold", 0.0)
            .withNumber("randomJoins", 42)
            .withNumber("topK", 1)
            .withEntry("nodeProperties", List.of("knn"));
    }

    @Test
    void failOnInvalidConfigurationParams() {
        String graphName = "graph";
        loadGraph(graphName);
        var configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
        applyOnProcedure(proc -> assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> {
                CypherMapWrapper invalidConfig = CypherMapWrapper.create(configMap)
                    .withNumber("topK", 0)
                    .withNumber("sampleRate", 0.0);
                proc.configParser().processInput(invalidConfig.toMap());
            })
            .withMessageContainingAll("`topK`", "0", "[1, 2147483647]")
            .withMessageContainingAll("`sampleRate`", "0.00", "(0.00, 1.00]"));
    }

    @Disabled
    @Override
    public void testRunOnEmptyGraph() {}
}
