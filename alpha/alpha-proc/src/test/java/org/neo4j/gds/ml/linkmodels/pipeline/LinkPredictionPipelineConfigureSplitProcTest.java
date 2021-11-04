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

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineConfigureParamsProcTest.DEFAULT_PARAM_CONFIG;

@ModelCatalogExtension
class LinkPredictionPipelineConfigureSplitProcTest extends BaseProcTest {

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineConfigureSplitProc.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')");
    }

    @Test
    void shouldOverrideSingleSplitField() {
        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('myPipeline', {validationFolds: 42})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", Map.of("negativeSamplingRatio", 1.0, "testFraction", 0.1, "validationFolds", 42, "trainFraction", 0.1),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void shouldOnlyKeepLastOverride() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('myPipeline', {validationFolds: 42})");

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('myPipeline', {testFraction: 0.5})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", Map.of("negativeSamplingRatio", 1.0, "testFraction", 0.5, "validationFolds", 3, "trainFraction", 0.1),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void failOnInvalidFractions() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('myPipeline', {testFraction: 0.5, trainFraction: 0.51})",
            "Sum of fractions for test and train set must be smaller than or equal to 1.0. But got 1.01."
        );
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('myPipeline', {invalidKey: 42, traiXFraction: -0.51})",
            "Unexpected configuration keys: invalidKey, traiXFraction (Did you mean [trainFraction]?"
        );
    }
}
