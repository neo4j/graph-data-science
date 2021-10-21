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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.pipelinecommon.PipelineDummyTrainConfig;
import org.neo4j.gds.model.catalog.ModelListProc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCreateProc.PIPELINE_MODEL_TYPE;

public class NodeClassificationPipelineCreateProcTest extends BaseProcTest {

    //TODO: move these to analogous places of those for LP
    static final Map<String, Object> DEFAULT_SPLIT_CONFIG =  Map.of("holdoutFraction", 0.3, "validationFolds", 3);
    static final List<Map<String, Object>> DEFAULT_PARAM_CONFIG = List.of(Map.of(
        "maxEpochs", 100,
        "minEpochs", 1,
        "penalty", 0.0,
        "patience", 1,
        "batchSize", 100,
        "tolerance", 0.001,
        "concurrency", 4
    ));

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeClassificationPipelineCreateProc.class, ModelListProc.class);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void createPipeline() {
        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.nodeClassification.create('myPipeline')",
            List.of(Map.of(
                "name", "myPipeline",
                "nodePropertySteps", List.of(),
                "featureSteps", List.of(),
                "splitConfig", DEFAULT_SPLIT_CONFIG,
                "parameterSpace", DEFAULT_PARAM_CONFIG
            ))
        );

        assertCypherResult(
            "CALL gds.beta.model.list('myPipeline')",
            singletonList(
                map(
                    "modelInfo", map(
                        "modelName", "myPipeline",
                        "modelType", PIPELINE_MODEL_TYPE,
                        "featurePipeline", Map.of(
                            "nodePropertySteps", List.of(),
                            "featureSteps", List.of()
                        ),
                        "splitConfig", DEFAULT_SPLIT_CONFIG,
                        "parameterSpace", DEFAULT_PARAM_CONFIG
                    ),
                    "trainConfig", PipelineDummyTrainConfig.of(getUsername()).toMap(),
                    "graphSchema", GraphSchema.empty().toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                )
            )
        );
    }

    @Test
    void failOnCreatingPipelineWithExistingName() {
        runQuery("CALL gds.alpha.ml.pipeline.nodeClassification.create('myPipeline')");
        assertError("CALL gds.alpha.ml.pipeline.nodeClassification.create('myPipeline')", "Model with name `myPipeline` already exists.");
    }

    @Test
    void failOnCreatingPipelineWithInvalidName() {
        assertError("CALL gds.alpha.ml.pipeline.nodeClassification.create(' ')",
            "`pipelineName` must not end or begin with whitespace characters, but got ` `.");
    }
}
