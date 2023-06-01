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
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class PipelineDropProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(PipelineDropProc.class);
    }

    @AfterEach
    void cleanUp() {
        PipelineCatalog.removeAll();
    }

    @Test
    void dropsPipeline() {
        String pipeName = "testPipe";
        var pipe = new NodeClassificationTrainingPipeline();

        PipelineCatalog.set(
            getUsername(),
            pipeName,
            pipe
        );

        var query = "gds.beta.pipeline.drop($pipelineName)";
        assertCypherResult(
            formatWithLocale(
                "CALL %s YIELD pipelineInfo, pipelineName, pipelineType, creationTime " +
                "RETURN pipelineInfo.splitConfig, pipelineName, pipelineType, creationTime",
                query
            ),
            Map.of("pipelineName", pipeName),
            List.of(
                Map.of(
                    "pipelineInfo.splitConfig", Map.of(
                        "testFraction", 0.3,
                        "validationFolds", 3
                    ),
                    "creationTime", isA(ZonedDateTime.class),
                    "pipelineName", pipeName,
                    "pipelineType", NodeClassificationTrainingPipeline.PIPELINE_TYPE
                )
            )
        );
    }

    @Test
    void failOnDroppingNonExistingPipeline() {
        String pipelineName = "foo";
        assertError(
            "CALL gds.beta.pipeline.drop($pipelineName)",
            Map.of("pipelineName", pipelineName),
            formatWithLocale("Pipeline with name `%s` does not exist for user `%s`.", pipelineName, getUsername())
        );
    }

    @Test
    void failSilentlyOnDroppingNonExistingPipeline() {
        assertCypherResult("CALL gds.beta.pipeline.drop('foo', false)", List.of());
    }
}
