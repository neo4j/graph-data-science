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
package org.neo4j.gds.procedures.pipelines;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.User;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class PipelinesProcedureFacadeTest {
    @Test
    void createPipeline() {
        var repository = new PipelineRepository();
        var nodeClassificationPredictPipelineEstimator = new NodeClassificationPredictPipelineEstimator(null, null);
        var applications = new PipelineApplications(
            null,
            null,
            null,
            repository,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new User("me", false),
            null,
            null,
            null,
            nodeClassificationPredictPipelineEstimator,
            null,
            null,
            null,
            null,
            null
        );
        var facade = new PipelinesProcedureFacade(null, null, applications);

        var result = facade.createPipeline("myPipeline").findAny().orElseThrow();

        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.nodePropertySteps).isEqualTo(List.of());
        assertThat(result.featureProperties).isEqualTo(List.of());
        assertThat(result.splitConfig).isEqualTo(NodeClassificationPipelineCompanion.DEFAULT_SPLIT_CONFIG);
        assertThat(result.parameterSpace).isEqualTo(NodeClassificationPipelineCompanion.DEFAULT_PARAM_CONFIG);

        assertThat(repository.exists(new User("me", false), PipelineName.parse("myPipeline"))).isTrue();
    }

    @Test
    void shouldNotCreatePipelineWhenOneExists() {
        var repository = new PipelineRepository();
        var nodeClassificationPredictPipelineEstimator = new NodeClassificationPredictPipelineEstimator(null, null);
        var applications = new PipelineApplications(
            null,
            null,
            null,
            repository,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new User("me", false),
            null,
            null,
            null,
            nodeClassificationPredictPipelineEstimator,
            null,
            null,
            null,
            null,
            null
        );
        var facade = new PipelinesProcedureFacade(null, null, applications);

        assertThatIllegalStateException()
            .isThrownBy(() -> facade.createPipeline("myPipeline"))
            .withMessage("Pipeline named `myPipeline` already exists.");
    }

    @Test
    void shouldNotCreatePipelineWithInvalidName() {
        var facade = new PipelinesProcedureFacade(
            null,
            null,
            null
        );

        assertThatIllegalArgumentException()
            .isThrownBy(() -> facade.createPipeline("   blanks!"))
            .withMessage("`pipelineName` must not end or begin with whitespace characters, but got `   blanks!`.");
    }
}
