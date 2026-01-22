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
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeRegressionPipelineCreateProcTest extends NodeRegressionPipelineBaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeRegressionPipelineCreateProc.class);
    }

    @Test
    void createPipeline() {
        assertCypherResult("CALL gds.alpha.pipeline.nodeRegression.create('p')", List.of(Map.of(
            "name", "p",
            "nodePropertySteps", List.of(),
            "featureProperties", List.of(),
            "splitConfig", NodePropertyPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
            "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap(),
            "parameterSpace", DEFAULT_PARAMETERSPACE
        )));

        assertThat(PipelineCatalog.exists(getUsername(), "p")).isTrue();
    }
}
