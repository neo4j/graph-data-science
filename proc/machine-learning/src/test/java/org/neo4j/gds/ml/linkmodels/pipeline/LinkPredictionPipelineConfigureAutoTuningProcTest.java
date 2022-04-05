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
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.DEFAULT_PARAM_SPACE;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;

class LinkPredictionPipelineConfigureAutoTuningProcTest extends BaseProcTest {

    private static final String PIPELINE_NAME = "myPipeline";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPipelineConfigureAutoTuningProc.class, LinkPredictionPipelineCreateProc.class);

        runQuery("CALL gds.beta.pipeline.linkPrediction.create($pipelineName)", Map.of("pipelineName", PIPELINE_NAME));
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldApplyDefaultMaxTrials() {
        String pipelineName = "confetti";
        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.create($pipelineName)",
            Map.of("pipelineName", pipelineName),
            List.of(Map.of(
                "name", "confetti",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
        var pipeline = (LinkPredictionTrainingPipeline) PipelineCatalog.get(getUsername(), pipelineName);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(MAX_TRIALS);
    }

    @Test
    void shouldOverrideSingleSplitField() {
        int maxTrials = 42;
        assertCypherResult(
            "CALL gds.alpha.pipeline.linkPrediction.configureAutoTuning('myPipeline', {maxTrials: $maxTrials})",
            Map.of("pipelineName", PIPELINE_NAME, "maxTrials", maxTrials),
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", maxTrials),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
        var pipeline = (LinkPredictionTrainingPipeline) PipelineCatalog.get(getUsername(), PIPELINE_NAME);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(maxTrials);
    }

    @Test
    void shouldOnlyKeepLastOverride() {
        int lastMaxTrials = 42;
        runQuery(
            "CALL gds.alpha.pipeline.linkPrediction.configureAutoTuning($pipelineName, {maxTrials: 1337})",
            Map.of("pipelineName", PIPELINE_NAME)
        );
        assertCypherResult(
            "CALL gds.alpha.pipeline.linkPrediction.configureAutoTuning($pipelineName, {maxTrials: $maxTrials})",
            Map.of("pipelineName", PIPELINE_NAME, "maxTrials", lastMaxTrials),
            List.of(Map.of(
                "name", "myPipeline",
                "splitConfig", LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 42),
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "parameterSpace", DEFAULT_PARAM_SPACE
            ))
        );
        var pipeline = (LinkPredictionTrainingPipeline) PipelineCatalog.get(getUsername(), PIPELINE_NAME);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(lastMaxTrials);
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.alpha.pipeline.linkPrediction.configureAutoTuning('myPipeline', {invalidKey: 42, maxTr1als: -0.51})",
            "Unexpected configuration keys: invalidKey, maxTr1als (Did you mean [maxTrials]?"
        );
    }
}
