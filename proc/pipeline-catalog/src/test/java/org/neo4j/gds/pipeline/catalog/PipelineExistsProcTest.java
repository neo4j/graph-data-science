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

import java.util.Map;

import static java.util.Collections.singletonList;

class PipelineExistsProcTest extends BaseProcTest {

    private static final String EXISTS_QUERY = "CALL gds.beta.pipeline.exists($pipelineName)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(PipelineExistsProc.class);
    }

    @AfterEach
    void cleanUp() {
        PipelineCatalog.removeAll();
    }

    @Test
    void checksIfPipelineExists() {
        String pipeName = "testPipe";
        var pipe = new NodeClassificationTrainingPipeline();

        PipelineCatalog.set(
            getUsername(),
            pipeName,
            pipe
        );

        assertCypherResult(
            EXISTS_QUERY,
            Map.of(
                "pipelineName", pipeName
            ),
            singletonList(
                Map.of(
                    "pipelineName", pipeName,
                    "pipelineType", NodeClassificationTrainingPipeline.PIPELINE_TYPE,
                    "exists", true
                )
            )
        );
    }

    @Test
    void returnsCorrectResultForNonExistingPipeline() {
        String bogusPipe = "bogusPipe";

        assertCypherResult(
            EXISTS_QUERY,
            Map.of(
                "pipelineName", bogusPipe),
            singletonList(
                Map.of(
                    "pipelineName", bogusPipe,
                    "pipelineType", "n/a",
                    "exists", false
                )
            )
        );
    }
}
