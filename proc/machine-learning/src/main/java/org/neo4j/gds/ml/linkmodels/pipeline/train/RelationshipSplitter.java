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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.executor.ProcedureExecutorSpec;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.ImmutableSplitRelationshipsMutateConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsAlgorithmFactory;
import org.neo4j.gds.ml.splitting.SplitRelationshipsBaseConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateConfig;
import org.neo4j.gds.ml.splitting.SplitRelationshipsMutateProc;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RelationshipSplitter {

    private static final String SPLIT_ERROR_TEMPLATE = "%s graph contains no relationships. Consider increasing the `%s` or provide a larger graph";

    private final String graphName;
    private final LinkPredictionSplitConfig splitConfig;
    private final ExecutionContext executionContext;
    private final ProgressTracker progressTracker;

    RelationshipSplitter(
        String graphName,
        LinkPredictionSplitConfig splitConfig,
        ExecutionContext executionContext,
        ProgressTracker progressTracker
    ) {
        this.graphName = graphName;
        this.splitConfig = splitConfig;
        this.executionContext = executionContext;
        this.progressTracker = progressTracker;
    }

    public void splitRelationships(
        GraphStore graphStore,
        List<String> relationshipTypes,
        List<String> nodeLabels,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        progressTracker.beginSubTask();

        splitConfig.validateAgainstGraphStore(graphStore);

        var testComplementRelationshipType = splitConfig.testComplementRelationshipType();

        // Relationship sets: test, train, feature-input, test-complement. The nodes are always the same.
        // 1. Split base graph into test, test-complement
        //      Test also includes newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.testSplit(), nodeLabels, relationshipTypes, randomSeed, relationshipWeightProperty);
        validateTestSplit(graphStore);


        // 2. Split test-complement into (labeled) train and feature-input.
        //      Train relationships also include newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.trainSplit(), nodeLabels, List.of(testComplementRelationshipType), randomSeed, relationshipWeightProperty);

        graphStore.deleteRelationships(RelationshipType.of(testComplementRelationshipType));

        progressTracker.endSubTask();
    }

    private void validateTestSplit(GraphStore graphStore) {
        String testRelationshipType = splitConfig.testRelationshipType();
        if (graphStore.getGraph(RelationshipType.of(testRelationshipType)).relationshipCount() <= 0) {
            throw new IllegalStateException(formatWithLocale(
                SPLIT_ERROR_TEMPLATE,
                "Test",
                LinkPredictionSplitConfig.TEST_FRACTION_KEY
            ));
        }
    }

    private void relationshipSplit(
        SplitRelationshipsBaseConfig splitConfig,
        List<String> nodeLabels,
        List<String> relationshipTypes,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        var splitRelationshipProcConfig = new HashMap<>(splitConfig.toSplitMap()) {{
            put("nodeLabels", nodeLabels);
            put("relationshipTypes", relationshipTypes);
            relationshipWeightProperty.ifPresent(s -> put(RELATIONSHIP_WEIGHT_PROPERTY, s));
            randomSeed.ifPresent(seed -> put("randomSeed", seed));
        }};

        var splitRelationshipsMutateProc = new SplitRelationshipsMutateProc();

        new ProcedureExecutor<>(
            splitRelationshipsMutateProc,
            new ProcedureExecutorSpec<>(),
            executionContext
        ).compute(graphName, splitRelationshipProcConfig, false, false);
    }

    static MemoryEstimation splitEstimation(LinkPredictionSplitConfig splitConfig, List<String> relationshipTypes) {
        List<String> checkRelTypes = relationshipTypes
            .stream()
            .map(type -> type.equals(ElementProjection.PROJECT_ALL) ? RelationshipType.ALL_RELATIONSHIPS.name : type)
            .collect(Collectors.toList());

        SplitRelationshipsMutateConfig testSplitConfig =  ImmutableSplitRelationshipsMutateConfig.builder()
            .from(splitConfig.testSplit())
            .relationshipTypes(checkRelTypes)
            .build();

        var firstSplitEstimation = MemoryEstimations
            .builder("Test/Test-complement split")
            .add(new SplitRelationshipsAlgorithmFactory().memoryEstimation(testSplitConfig))
            .build();

        SplitRelationshipsMutateConfig trainSplitConfig = ImmutableSplitRelationshipsMutateConfig.builder()
            .from(splitConfig.trainSplit())
            .relationshipTypes(List.of(splitConfig.testComplementRelationshipType()))
            .build();

        var secondSplitEstimation = MemoryEstimations
            .builder("Train/Feature-input split")
            .add(new SplitRelationshipsAlgorithmFactory().memoryEstimation(trainSplitConfig))
            .build();

        return MemoryEstimations.builder("Split relationships")
            .add(firstSplitEstimation)
            .add(secondSplitEstimation)
            .build();
    }
}
