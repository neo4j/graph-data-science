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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AlgorithmProcessingTemplateConvenience {
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public AlgorithmProcessingTemplateConvenience(AlgorithmProcessingTemplate algorithmProcessingTemplate) {
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    /**
     * With all bells and whistles
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER processAlgorithm(
        Optional<String> relationshipWeightOverride,
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            label,
            estimationFactory,
            algorithmComputation,
            mutateOrWriteStep,
            resultBuilder
        );
    }

    /**
     * No relationship weight override, no validation hooks
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER processRegularAlgorithmInMutateOrWriteMode(
        GraphName graphName,
        CONFIGURATION configuration,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> mutateStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            estimationFactory,
            algorithmComputation,
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    /**
     * No relationship weight override, no validation hooks, no mutate or write step
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER processRegularAlgorithmInStatsOrStreamMode(
        GraphName graphName,
        CONFIGURATION configuration,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            estimationFactory,
            algorithmComputation,
            Optional.empty(),
            resultBuilder
        );
    }

    public <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM> Stream<RESULT_TO_CALLER> processRegularAlgorithmInStreamMode(
        GraphName graphName,
        CONFIGURATION configuration,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        StreamResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder,
        Optional<Iterable<PostLoadValidationHook>> postGraphStoreLoadValidationHooks,
        Optional<String> relationshipWeightOverride
        ) {
        return algorithmProcessingTemplate.processAlgorithmForStream(
            relationshipWeightOverride,
            graphName,
            configuration,
            postGraphStoreLoadValidationHooks,
            label,
            estimationFactory,
            algorithmComputation,
            resultBuilder
        );
    }
}
