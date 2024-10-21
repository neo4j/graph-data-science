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

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public interface NodeClassificationFacade {
    Stream<NodePipelineInfoResult> addLogisticRegression(
        String pipelineName,
        Map<String, Object> configuration
    );

    Stream<NodePipelineInfoResult> addMLP(String pipelineName, Map<String, Object> configuration);

    Stream<NodePipelineInfoResult> addNodeProperty(
        String pipelineNameAsString,
        String taskName,
        Map<String, Object> procedureConfig
    );

    Stream<NodePipelineInfoResult> addRandomForest(String pipelineName, Map<String, Object> configuration);

    Stream<NodePipelineInfoResult> configureAutoTuning(String pipelineName, Map<String, Object> configuration);

    Stream<NodePipelineInfoResult> configureSplit(String pipelineName, Map<String, Object> configuration);

    Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString);

    Stream<PredictMutateResult> mutate(
        String graphNameAsString,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject);

    Stream<NodeClassificationStreamResult> stream(
        String graphNameAsString,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<NodeClassificationPipelineTrainResult> train(
        String graphNameAsString,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> trainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );

    Stream<WriteResult> write(String graphNameAsString, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    );
}
