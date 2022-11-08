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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.negativeSampling.NegativeSampler;
import org.neo4j.gds.ml.pipeline.linkPipeline.ExpectedSetSizes;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.ml.splitting.UndirectedEdgeSplitter;
import org.neo4j.values.storable.NumberType;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_TEST_COMPLEMENT_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_TRAIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.validateRelSetSize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionRelationshipSampler {

    private final LinkPredictionSplitConfig splitConfig;
    private LinkPredictionTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final GraphStore graphStore;

     public LinkPredictionRelationshipSampler(
         GraphStore graphStore,
         LinkPredictionSplitConfig splitConfig,
         LinkPredictionTrainConfig trainConfig,
         ProgressTracker progressTracker
     ) {
        this.graphStore = graphStore;
        this.splitConfig = splitConfig;
        this.trainConfig = trainConfig;
        this.progressTracker = progressTracker;
    }

    @NotNull
    static LeafTask progressTask(ExpectedSetSizes sizes) {
        return Tasks.leaf(
            "Split relationships",
            sizes.trainSize() + sizes.featureInputSize() + sizes.testSize() + sizes.testComplementSize()
        );
    }

    public void splitAndSampleRelationships(
        Optional<String> relationshipWeightProperty
    ) {
        progressTracker.beginSubTask("Split relationships");

        splitConfig.validateAgainstGraphStore(graphStore, trainConfig.internalTargetRelationshipType());

        if (trainConfig.sourceNodeLabel().equals(ElementProjection.PROJECT_ALL) || trainConfig.targetNodeLabel().equals(ElementProjection.PROJECT_ALL)) {
            progressTracker.logWarning(formatWithLocale(
                "Using %s for the `sourceNodeLabel` or `targetNodeLabel` results in not ideal negative link sampling.",
                ElementProjection.PROJECT_ALL
            ));
        }

        var testComplementRelationshipType = splitConfig.testComplementRelationshipType();

        var sourceLabels = ElementTypeValidator.resolve(graphStore, List.of(trainConfig.sourceNodeLabel()));
        var targetLabels = ElementTypeValidator.resolve(graphStore, List.of(trainConfig.targetNodeLabel()));
        IdMap sourceNodes = graphStore.getGraph(sourceLabels);
        IdMap targetNodes = graphStore.getGraph(targetLabels);
        var graph = graphStore.getGraph(
            trainConfig.nodeLabelIdentifiers(graphStore),
            trainConfig.internalRelationshipTypes(graphStore),
            relationshipWeightProperty);

        // Relationship sets: test, train, feature-input, test-complement. The nodes are always the same.
        // 1. Split base graph into test, test-complement
        var testSplitResult = split(sourceNodes, targetNodes, graph, relationshipWeightProperty, splitConfig.testComplementRelationshipType(), splitConfig.testFraction());

        // 2. Split test-complement into (labeled) train and feature-input.
        var testComplementGraph = graphStore.getGraph(
            trainConfig.nodeLabelIdentifiers(graphStore),
            List.of(splitConfig.testComplementRelationshipType()),
            relationshipWeightProperty
        );
        var trainSplitResult = split(sourceNodes, targetNodes, testComplementGraph, relationshipWeightProperty, splitConfig.featureInputRelationshipType(), splitConfig.trainFraction());

        // 3. add negative examples to test and train
        NegativeSampler negativeSampler = NegativeSampler.of(
            graphStore,
            graph,
            splitConfig.negativeRelationshipType(),
            splitConfig.negativeSamplingRatio(),
            testSplitResult.selectedRelCount(),
            trainSplitResult.selectedRelCount(),
            sourceNodes,
            targetNodes,
            trainConfig.randomSeed()
        );
        negativeSampler.produceNegativeSamples(testSplitResult.selectedRels(), trainSplitResult.selectedRels());

        // 4. Update graphStore with (positive+negative) 'TEST' and 'TRAIN' edges
        graphStore.addRelationshipType(
            splitConfig.testRelationshipType(),
            Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY),
            Optional.of(NumberType.INTEGRAL),
            testSplitResult.selectedRels().build()
        );
        graphStore.addRelationshipType(
            splitConfig.trainRelationshipType(),
            Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY),
            Optional.of(NumberType.INTEGRAL),
            trainSplitResult.selectedRels().build()
        );

        validateTestSplit(graphStore);
        validateTrainSplit(graphStore);
        graphStore.deleteRelationships(testComplementRelationshipType);

        progressTracker.endSubTask("Split relationships");
    }

    private EdgeSplitter.SplitResult split(IdMap sourceNodes, IdMap targetNodes, Graph graph, Optional<String> relationshipWeightProperty, RelationshipType remainingRelType, double selectedFraction) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires graph to be UNDIRECTED");
        }
        var splitter = new UndirectedEdgeSplitter(
            trainConfig.randomSeed(),
            sourceNodes,
            targetNodes,
            ConcurrencyConfig.DEFAULT_CONCURRENCY
        );

        var splitResult = splitter.splitPositiveExamples(
            graph,
            selectedFraction
        );

        var remainingRels = splitResult.remainingRels().build();
        graphStore.addRelationshipType(
            remainingRelType,
            relationshipWeightProperty,
            Optional.of(NumberType.FLOATING_POINT),
            remainingRels
        );

        return splitResult;
    }


    private void validateTestSplit(GraphStore graphStore) {
        validateRelSetSize(graphStore.relationshipCount(splitConfig.testRelationshipType()), MIN_SET_SIZE, "test", "`testFraction` is too low");
        validateRelSetSize(graphStore.relationshipCount(splitConfig.testComplementRelationshipType()), MIN_TEST_COMPLEMENT_SET_SIZE, "test-complement", "`testFraction` is too high");
    }

    private void validateTrainSplit(GraphStore graphStore) {
         // needs to validate these here as the filter can reduce the actual set size from the expected set size
        validateRelSetSize(graphStore.relationshipCount(splitConfig.trainRelationshipType()), MIN_TRAIN_SET_SIZE, "train", "`trainFraction` is too low");
        validateRelSetSize(graphStore.relationshipCount(splitConfig.featureInputRelationshipType()), MIN_SET_SIZE, "feature-input", "`trainFraction` is too high");
        validateRelSetSize(
            graphStore.relationshipCount(splitConfig.trainRelationshipType()) / splitConfig.validationFolds(),
            MIN_SET_SIZE,
            "validation",
            "`validationFolds` is too high or the `trainFraction` too low"
        );
    }

    static MemoryEstimation splitEstimation(LinkPredictionSplitConfig splitConfig, String targetRelationshipType, Optional<String> relationshipWeight) {
        var checkTargetRelType = targetRelationshipType.equals(ElementProjection.PROJECT_ALL)
            ? RelationshipType.ALL_RELATIONSHIPS
            : RelationshipType.of(targetRelationshipType);

        // randomSeed does not matter for memory estimation
        Optional<Long> randomSeed = Optional.empty();

        var firstSplitEstimation = MemoryEstimations
            .builder("Test/Test-complement split")
            .add(estimate(
                checkTargetRelType.name,
                splitConfig.testFraction(),
                splitConfig.negativeSamplingRatio(),
                relationshipWeight
            ))
            .build();

        var secondSplitEstimation = MemoryEstimations
            .builder("Train/Feature-input split")
            .add(estimate(
                splitConfig.testComplementRelationshipType().name,
                splitConfig.trainFraction(),
                splitConfig.negativeSamplingRatio(),
                relationshipWeight
            ))
            .build();

        return MemoryEstimations.builder("Split relationships")
            .add(firstSplitEstimation)
            .add(secondSplitEstimation)
            .build();
    }

    public static MemoryEstimation estimate(String relationshipType, double holdoutFraction, double negativeSamplingRatio, Optional<String> relationshipWeight) {
        // we cannot assume any compression of the relationships
        var pessimisticSizePerRel = relationshipWeight.isPresent()
            ? Double.BYTES + 2 * Long.BYTES
            : 2 * Long.BYTES;

        return MemoryEstimations.builder("Relationship splitter")
            .perGraphDimension("Selected relationships", (graphDimensions, threads) -> {
                var positiveRelCount = graphDimensions.estimatedRelCount(List.of(relationshipType)) * holdoutFraction;
                var negativeRelCount = positiveRelCount * negativeSamplingRatio;
                long selectedRelCount = (long) (positiveRelCount + negativeRelCount);

                // Whether the graph is undirected or directed
                return MemoryRange.of(selectedRelCount / 2, selectedRelCount).times(pessimisticSizePerRel);
            })
            .perGraphDimension("Remaining relationships", (graphDimensions, threads) -> {
                long remainingRelCount = (long) (graphDimensions.estimatedRelCount(List.of(relationshipType)) * (1 - holdoutFraction));
                // remaining relationships are always undirected
                return MemoryRange.of(remainingRelCount * pessimisticSizePerRel);
            })
            .build();
    }
}
