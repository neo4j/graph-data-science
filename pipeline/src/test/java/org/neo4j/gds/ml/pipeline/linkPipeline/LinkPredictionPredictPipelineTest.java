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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.executor.GdsCallableFinder;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionPredictPipelineTest {

    @Test
    void copiesTheListFromTrainPipeline() {
        var trainPipeline = new LinkPredictionTrainingPipeline();

        GdsCallableFinder.GdsCallableDefinition callableDefinition = GdsCallableFinder
            .findByName("gds.testProc.mutate", List.of())
            .orElseThrow();
        var step = new NodePropertyStep(callableDefinition, Map.of("mutateProperty", "pr"));
        trainPipeline.addNodePropertyStep(step);

        trainPipeline.addFeatureStep(new HadamardFeatureStep(List.of("a")));

        var predictPipeline = LinkPredictionPredictPipeline.from(trainPipeline);

        assertThat(predictPipeline.nodePropertySteps())
            .isNotSameAs(trainPipeline.nodePropertySteps())
            .containsExactlyElementsOf(trainPipeline.nodePropertySteps());

        assertThat(predictPipeline.featureSteps())
            .isNotSameAs(trainPipeline.featureSteps())
            .containsExactlyElementsOf(trainPipeline.featureSteps());
    }

}
