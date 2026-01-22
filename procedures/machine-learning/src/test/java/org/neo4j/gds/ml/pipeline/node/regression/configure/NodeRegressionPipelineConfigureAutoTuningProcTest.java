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
package org.neo4j.gds.ml.pipeline.node.regression.configure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;

class NodeRegressionPipelineConfigureAutoTuningProcTest extends NodeRegressionPipelineBaseProcTest {

    private static final String PIPE_NAME = "myPipe";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            NodeRegressionPipelineCreateProc.class,
            NodeRegressionPipelineConfigureAutoTuningProc.class
        );

        runQuery("CALL gds.alpha.pipeline.nodeRegression.create($name)", Map.of("name", PIPE_NAME));
    }

    @Test
    void shouldApplyDefaultMaxTrials() {
        var pipeline = PipelineCatalog.get(getUsername(), PIPE_NAME);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(MAX_TRIALS);
    }

    @Test
    void shouldOverrideSingleSplitField() {
        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.configureAutoTuning($name, {maxTrials: 42})",
            Map.of("name", PIPE_NAME),
            List.of(Map.of(
                "name", PIPE_NAME,
                "splitConfig", NodePropertyPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 42),
                "nodePropertySteps", List.of(),
                "featureProperties", List.of(),
                "parameterSpace", DEFAULT_PARAMETERSPACE
            ))
        );
        var pipeline = PipelineCatalog.get(getUsername(), PIPE_NAME);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(42);
    }

    @Test
    void shouldOnlyKeepLastOverride() {
        runQuery(
            "CALL gds.alpha.pipeline.nodeRegression.configureAutoTuning($name, {maxTrials: 1337})",
            Map.of("name", PIPE_NAME)
        );

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.configureAutoTuning($name, {maxTrials: 42})",
            Map.of("name", PIPE_NAME),
            List.of(Map.of(
                "name", PIPE_NAME,
                "splitConfig", NodePropertyPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "autoTuningConfig", Map.of("maxTrials", 42),
                "nodePropertySteps", List.of(),
                "featureProperties", List.of(),
                "parameterSpace", DEFAULT_PARAMETERSPACE
            ))
        );
        var pipeline = PipelineCatalog.get(getUsername(), PIPE_NAME);
        assertThat(pipeline.autoTuningConfig().maxTrials()).isEqualTo(42);
    }

    @Test
    void failOnInvalidKeys() {
        assertError(
            "CALL gds.alpha.pipeline.nodeRegression.configureAutoTuning($name, {invalidKey: 42, maxTr1als: -0.51})",
            Map.of("name", PIPE_NAME),
            "Unexpected configuration keys: invalidKey, maxTr1als (Did you mean [maxTrials]?"
        );
    }
}
