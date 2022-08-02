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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.pipeline.linkPipeline.ExpectedSetSizes;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.ml.splitting.SplitRelationships;
import org.neo4j.gds.ml.splitting.SplitRelationshipsBaseConfig;

import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RelationshipSplitter {

    private static final String SPLIT_ERROR_TEMPLATE = "%s graph contains no relationships. Consider increasing the `%s` or provide a larger graph";

    private final LinkPredictionSplitConfig splitConfig;
    private final ProgressTracker progressTracker;
    private final GraphStore graphStore;
    private final TerminationFlag terminationFlag;

     public RelationshipSplitter(
         GraphStore graphStore,
         LinkPredictionSplitConfig splitConfig,
         ProgressTracker progressTracker,
         TerminationFlag terminationFlag
     ) {
        this.graphStore = graphStore;
        this.splitConfig = splitConfig;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @NotNull
    static LeafTask progressTask(ExpectedSetSizes sizes) {
        return Tasks.leaf(
            "Split relationships",
            sizes.trainSize() + sizes.featureInputSize() + sizes.testSize() + sizes.testComplementSize()
        );
    }

    public void splitRelationships(
        RelationshipType targetRelationshipType,
        String sourceNodeLabel,
        String targetNodeLabel,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        progressTracker.beginSubTask("Split relationships");

        splitConfig.validateAgainstGraphStore(graphStore);

        if (sourceNodeLabel.equals(ElementProjection.PROJECT_ALL) || targetNodeLabel.equals(ElementProjection.PROJECT_ALL)) {
            progressTracker.logWarning(formatWithLocale(
                "Using %s for the `sourceNodeLabel` or `targetNodeLabel` results in not ideal negative link sampling.",
                ElementProjection.PROJECT_ALL
            ));
        }

        var testComplementRelationshipType = splitConfig.testComplementRelationshipType();


        // Relationship sets: test, train, feature-input, test-complement. The nodes are always the same.
        // 1. Split base graph into test, test-complement
        //      Test also includes newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.testSplit(targetRelationshipType, sourceNodeLabel, targetNodeLabel, randomSeed, relationshipWeightProperty));
        validateTestSplit(graphStore);


        // 2. Split test-complement into (labeled) train and feature-input.
        //      Train relationships also include newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.trainSplit(targetRelationshipType, sourceNodeLabel, targetNodeLabel, randomSeed, relationshipWeightProperty));

        graphStore.deleteRelationships(testComplementRelationshipType);

        progressTracker.endSubTask("Split relationships");
    }

    private void validateTestSplit(GraphStore graphStore) {
        if (graphStore.getGraph(splitConfig.testRelationshipType()).relationshipCount() <= 0) {
            throw new IllegalStateException(formatWithLocale(
                SPLIT_ERROR_TEMPLATE,
                "Test",
                LinkPredictionSplitConfig.TEST_FRACTION_KEY
            ));
        }
    }

    private void relationshipSplit(SplitRelationshipsBaseConfig splitConfig) {
        // the split config is generated internally and the input should be fully validated already
        splitConfig.graphStoreValidation(graphStore, splitConfig.nodeLabelIdentifiers(graphStore), splitConfig.internalRelationshipTypes(graphStore));

        var splitAlgo = SplitRelationships.of(graphStore, splitConfig);
        splitAlgo.setTerminationFlag(terminationFlag);

        EdgeSplitter.SplitResult result = splitAlgo.compute();

        SplitRelationshipGraphStoreMutator.mutate(graphStore, result, splitConfig);
    }

    static MemoryEstimation splitEstimation(LinkPredictionSplitConfig splitConfig, String targetRelationshipType, Optional<String> relationshipWeight, String sourceNodeLabel, String targetNodeLabel) {
        var checkTargetRelType = targetRelationshipType.equals(ElementProjection.PROJECT_ALL)
            ? RelationshipType.ALL_RELATIONSHIPS
            : RelationshipType.of(targetRelationshipType);

        // randomSeed does not matter for memory estimation
        Optional<Long> randomSeed = Optional.empty();

        var firstSplitEstimation = MemoryEstimations
            .builder("Test/Test-complement split")
            .add(SplitRelationships.estimate(splitConfig.testSplit(
                checkTargetRelType,
                sourceNodeLabel,
                targetNodeLabel,
                randomSeed,
                relationshipWeight
            )))
            .build();

        var secondSplitEstimation = MemoryEstimations
            .builder("Train/Feature-input split")
            .add(SplitRelationships.estimate(splitConfig.trainSplit(
                checkTargetRelType,
                sourceNodeLabel,
                targetNodeLabel,
                randomSeed,
                relationshipWeight
            )))
            .build();

        return MemoryEstimations.builder("Split relationships")
            .add(firstSplitEstimation)
            .add(secondSplitEstimation)
            .build();
    }
}
