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
package org.neo4j.gds.pipeline.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class PipelineListProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(PipelineListProc.class);
    }

    @AfterEach
    void cleanUp() {
        PipelineCatalog.removeAll();
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.beta.pipeline.list()", "gds.beta.pipeline.list(null)"})
    void listsPipelines(String query) {
        var ncPipe1 = new NodeClassificationTrainingPipeline();
        ncPipe1.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.8)
            .build()
        );
        var ncPipe2 = new NodeClassificationTrainingPipeline();
        var lpPipe = new LinkPredictionTrainingPipeline();

        PipelineCatalog.set(getUsername(), "ncPipe1", ncPipe1);
        PipelineCatalog.set(getUsername(), "ncPipe2", ncPipe2);
        PipelineCatalog.set(getUsername(), "lpPipe", lpPipe);

        assertCypherResult(
            formatWithLocale(
                "CALL %s YIELD pipelineInfo, pipelineName, pipelineType, creationTime " +
                "RETURN pipelineInfo.splitConfig, pipelineInfo.autoTuningConfig, pipelineName, pipelineType, creationTime " +
                "ORDER BY pipelineName",
                query
            ),
            List.of(
                Map.of(
                    "pipelineInfo.splitConfig", Map.of(
                        "negativeSamplingRatio", 1.0,
                        "testFraction", 0.1,
                        "validationFolds", 3,
                        "trainFraction", 0.1
                    ),
                    "pipelineInfo.autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                    "creationTime", isA(ZonedDateTime.class),
                    "pipelineName", "lpPipe",
                    "pipelineType", LinkPredictionTrainingPipeline.PIPELINE_TYPE
                ),
                Map.of(
                    "pipelineInfo.splitConfig", Map.of(
                        "testFraction", 0.8,
                        "validationFolds", 3
                    ),
                    "pipelineInfo.autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                    "creationTime", isA(ZonedDateTime.class),
                    "pipelineName", "ncPipe1",
                    "pipelineType", NodeClassificationTrainingPipeline.PIPELINE_TYPE
                ),
                Map.of(
                    "pipelineInfo.splitConfig", Map.of(
                        "testFraction", 0.3,
                        "validationFolds", 3
                    ),
                    "pipelineInfo.autoTuningConfig", Map.of("maxTrials", MAX_TRIALS),
                    "creationTime", isA(ZonedDateTime.class),
                    "pipelineName", "ncPipe2",
                    "pipelineType", NodeClassificationTrainingPipeline.PIPELINE_TYPE
                )
            )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CALL gds.beta.pipeline.list()",
        "CALL gds.beta.pipeline.list('somePipe')"
    })
    void emptyResultOnListQueries(String query) {
        assertCypherResult(
            query,
            List.of()
        );
    }

    @Test
    void returnSpecificPipeline() {
        var ncPipe1 = new NodeClassificationTrainingPipeline();
        ncPipe1.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.8)
            .build()
        );
        var ncPipe2 = new NodeClassificationTrainingPipeline();

        PipelineCatalog.set(getUsername(), "ncPipe1", ncPipe1);
        PipelineCatalog.set(getUsername(), "ncPipe2", ncPipe2);

        assertCypherResult(
            "CALL gds.beta.pipeline.list('ncPipe1') YIELD pipelineInfo, pipelineName, pipelineType, creationTime " +
            "RETURN pipelineInfo.splitConfig, pipelineName, pipelineType, creationTime " +
            "ORDER BY pipelineName",
            List.of(
                Map.of(
                    "pipelineInfo.splitConfig", Map.of(
                        "testFraction", 0.8,
                        "validationFolds", 3
                    ),
                    "creationTime", isA(ZonedDateTime.class),
                    "pipelineName", "ncPipe1",
                    "pipelineType", NodeClassificationTrainingPipeline.PIPELINE_TYPE
                )
            )
        );
    }

    static Stream<String> invalidPipeNames() {
        return Stream.of("", "   ", "           ", "\r\n\t");
    }

    @ParameterizedTest(name = "`{0}`")
    @MethodSource("invalidPipeNames")
    void failOnEmptyPipeName(String pipeName) {
        assertError(
            "CALL gds.beta.pipeline.list($pipeName)",
            Map.of("pipeName", pipeName),
            "can not be null or blank"
        );
    }
}
