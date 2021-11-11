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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.InjectModelCatalog;
import org.neo4j.gds.core.ModelCatalogExtension;
import org.neo4j.gds.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineAddStepProcsTest.DEFAULT_SPLIT_CONFIG;

@ModelCatalogExtension
class LinkPredictionPipelineConfigureParamsProcTest extends BaseProcTest {

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineConfigureParamsProc.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')");
    }

    @Test
    void shouldSetParams() {
        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('myPipeline', [{minEpochs: 42}])",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", List.of(Map.of(
                    "maxEpochs", 100,
                    "minEpochs", 42,
                    "penalty", 0.0,
                    "patience", 1,
                    "batchSize", 100,
                    "useBiasFeature", true,
                    "tolerance", 0.001
                ))
            ))
        );
    }

    @Test
    void shouldOnlyKeepLastOverride() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('myPipeline', [{minEpochs: 42}])");

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('myPipeline', [{minEpochs: 4}])",
            List.of(Map.of("name",
                "myPipeline",
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", List.of(Map.of(
                    "maxEpochs", 100,
                    "minEpochs", 4,
                    "penalty", 0.0,
                    "patience", 1,
                    "batchSize", 100,
                    "tolerance", 0.001,
                    "useBiasFeature", true
                ))
            ))
        );
    }

    @Test
    void failOnInvalidParameterValues() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('myPipeline', [{minEpochs: 0.5, batchSize: 0.51}])",
            "Multiple errors in configuration arguments:\n" +
            "\t\t\t\tThe value of `batchSize` must be of type `Integer` but was `Double`.\n" +
            "\t\t\t\tThe value of `minEpochs` must be of type `Integer` but was `Double`."
        );
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('myPipeline', [{invalidKey: 42, penaltE: -0.51}])",
            "Unexpected configuration keys: invalidKey, penaltE (Did you mean one of [penalty, patience]?)"
        );
    }

}
