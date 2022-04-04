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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.DEFAULT_PARAM_SPACE;

class LinkPredictionPipelineConfigureAutoTuningProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineConfigureAutoTuningProc.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('myPipeline')");
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldApplyDefaultMaxTrials() {
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.create('confetti')",
            List.of(Map.of(
                "name", "confetti",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 100),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void shouldOverrideSingleSplitField() {
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.configureAutoTuning('myPipeline', {maxTrials: 42})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 42),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void shouldOnlyKeepLastOverride() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureAutoTuning('myPipeline', {maxTrials: 1337})");
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.configureAutoTuning('myPipeline', {maxTrials: 42})",
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 42),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.configureAutoTuning('myPipeline', {invalidKey: 42, maxTr1als: -0.51})",
            "Unexpected configuration keys: invalidKey, maxTr1als (Did you mean [maxTrials]?"
        );
    }
}
