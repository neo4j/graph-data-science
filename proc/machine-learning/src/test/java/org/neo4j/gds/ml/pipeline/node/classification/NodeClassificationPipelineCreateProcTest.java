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
package org.neo4j.gds.ml.pipeline.node.classification;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.model.catalog.ModelListProc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeClassificationPipelineCreateProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeClassificationPipelineCreateProc.class, ModelListProc.class);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void createPipeline() {
        var result = NodeClassificationPipelineCreateProc.create(getUsername(), "myPipeline");
        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.nodePropertySteps).isEqualTo(List.of());
        assertThat(result.featureProperties).isEqualTo(List.of());
        assertThat(result.splitConfig).isEqualTo(NodeClassificationPipelineCompanion.DEFAULT_SPLIT_CONFIG);
        assertThat(result.parameterSpace).isEqualTo(NodeClassificationPipelineCompanion.DEFAULT_PARAM_CONFIG);

        assertThat(PipelineCatalog.exists(getUsername(), "myPipeline")).isTrue();
    }

    @Test
    void failOnCreatingPipelineWithExistingName() {
        NodeClassificationPipelineCreateProc.create(getUsername(), "myPipeline");
        assertThatThrownBy(() -> NodeClassificationPipelineCreateProc.create(getUsername(), "myPipeline"))
            .hasMessageContaining("Pipeline named `myPipeline` already exists.");
    }

    @Test
    void failOnCreatingPipelineWithInvalidName() {
        assertThatThrownBy(() -> NodeClassificationPipelineCreateProc.create(getUsername(), " "))
            .hasMessageContaining("`pipelineName` must not end or begin with whitespace characters, but got ` `.");
    }
}
