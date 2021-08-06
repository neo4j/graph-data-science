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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;

class LinkPredictionPipelineTrainProcTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";
    // Five cliques of size 2, 3, or 4
    @Neo4jGraph
    static String GRAPH =
        "CREATE " +
        "(a:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(b:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(c:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(d:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(e:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(f:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(g:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(h:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(i:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(j:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(k:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(l:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(m:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(n:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(o:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +

        "(a)-[:REL]->(b), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL]->(d), " +
        "(b)-[:REL]->(c), " +
        "(b)-[:REL]->(d), " +
        "(c)-[:REL]->(d), " +
        "(e)-[:REL]->(f), " +
        "(e)-[:REL]->(g), " +
        "(f)-[:REL]->(g), " +
        "(h)-[:REL]->(i), " +
        "(j)-[:REL]->(k), " +
        "(j)-[:REL]->(l), " +
        "(k)-[:REL]->(l), " +
        "(m)-[:REL]->(n), " +
        "(m)-[:REL]->(o), " +
        "(n)-[:REL]->(o) ";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            LinkPredictionPipelineTrainProc.class,
            LinkFeaturePipelineCreateProc.class,
            LinkFeaturePipelineAddStepProcs.class,
            LinkFeaturePipelineConfigureParamsProc.class,
            LinkFeaturePipelineConfigureSplitProc.class,
            GraphCreateProc.class
        );

        runQuery(GRAPH);

        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainAModel() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337 }" +
            ")",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of(
                    "modelInfo", Matchers.allOf(
                        Matchers.hasEntry("modelName", "trainedModel"),
                        Matchers.hasEntry("modelType", "Link prediction pipeline"),
                        Matchers.hasKey("bestParameters"),
                        Matchers.hasKey("metrics")
                    ),
                    "trainMillis", greaterThan(-1L),
                    "configuration", aMapWithSize(9)
                ))
        );
    }

    @Test
    void failsWhenMissingFeatures() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
                    "   $graphName, " +
                    "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337 }" +
                    ")",
            Map.of("graphName", GRAPH_NAME),
            "Training a Link prediction pipeline requires at least one feature. You can add features with the procedure `gds.alpha.ml.pipeline.linkPrediction.addFeature`.");

    }

}
