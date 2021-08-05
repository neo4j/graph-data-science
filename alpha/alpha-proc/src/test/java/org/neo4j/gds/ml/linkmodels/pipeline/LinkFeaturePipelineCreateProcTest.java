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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkFeaturePipelineAddStepProcsTest.DEFAULT_SPLIT_CONFIG;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkFeaturePipelineConfigureParamsProcTest.DEFAULT_PARAM_CONFIG;

public class LinkFeaturePipelineCreateProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkFeaturePipelineCreateProc.class);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void createPipeline() {
        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')",
            List.of(Map.of(
                "name", "myPipeline",
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );
    }

    @Test
    void failOnCreatingPipelineWithExistingName() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')");
        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')", "Model with name `myPipeline` already exists.");
    }

    @Disabled("White space models are allowed atm.")
    @Test
    void failOnCreatingPipelineWithInvalidName() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('   ')");
        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.create('myPipeline')", "Model with name must contain at least one non white-space character.");
    }
}
