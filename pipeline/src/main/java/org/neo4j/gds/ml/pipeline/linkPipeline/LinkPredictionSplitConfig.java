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


import org.immutables.value.Value;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_TEST_COMPLEMENT_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_TRAIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.validateRelSetSize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface LinkPredictionSplitConfig extends ToMapConvertible {

    String TEST_FRACTION_KEY = "testFraction";
    String TRAIN_FRACTION_KEY = "trainFraction";
    LinkPredictionSplitConfig DEFAULT_CONFIG = LinkPredictionSplitConfig.of(CypherMapWrapper.empty());

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int validationFolds() {
        return 3;
    }

    @Value.Default
    @Configuration.Key(TEST_FRACTION_KEY)
    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double testFraction() {
        return 0.1;
    }

    @Value.Default
    @Configuration.Key(TRAIN_FRACTION_KEY)
    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double trainFraction() {
        return 0.1;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double negativeSamplingRatio() {
        return 1.0;
    }

    Optional<String> negativeRelationshipType();

    @Value.Default
    @Configuration.Ignore
    default RelationshipType testRelationshipType() {
        return RelationshipType.of("_TEST_");
    }

    @Value.Default
    @Configuration.Ignore
    default RelationshipType testComplementRelationshipType() {
        return RelationshipType.of("_TEST_COMPLEMENT_");
    }

    @Value.Default
    @Configuration.Ignore
    default RelationshipType trainRelationshipType() {
        return RelationshipType.of("_TRAIN_");
    }

    @Value.Default
    @Configuration.Ignore
    default RelationshipType featureInputRelationshipType() {
        return RelationshipType.of("_FEATURE_INPUT_");
    }

    @Override
    @Configuration.ToMap
    Map<String, Object> toMap();

    @Configuration.CollectKeys
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    static LinkPredictionSplitConfig of(CypherMapWrapper config) {
        return new LinkPredictionSplitConfigImpl(config);
    }

    @Configuration.Ignore
    default void validateAgainstGraphStore(GraphStore graphStore, RelationshipType targetRelationshipType) {
        var reservedTypes = Stream.of(
            testRelationshipType(),
            trainRelationshipType(),
            featureInputRelationshipType(),
            testComplementRelationshipType()
        );

        var invalidTypes = reservedTypes
            .filter(graphStore::hasRelationshipType)
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!invalidTypes.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The relationship types %s are in the input graph, but are reserved for splitting.",
                StringJoining.join(invalidTypes)
            ));
        }

        var expectedSetSizes = expectedSetSizes(graphStore.relationshipCount(targetRelationshipType));

        validateRelSetSize(expectedSetSizes.testSize(), MIN_SET_SIZE, "test", "`testFraction` is too low");
        validateRelSetSize(
            expectedSetSizes.testComplementSize(),
            MIN_TEST_COMPLEMENT_SET_SIZE,
            "test-complement",
            "`testFraction` is too high"
        );
        validateRelSetSize(expectedSetSizes.trainSize(), MIN_TRAIN_SET_SIZE, "train", "`trainFraction` is too low");
        validateRelSetSize(expectedSetSizes.featureInputSize(), MIN_SET_SIZE, "feature-input", "`trainFraction` is too high");
        validateRelSetSize(
            expectedSetSizes.validationFoldSize(),
            MIN_SET_SIZE,
            "validation",
            "`validationFolds` is too high or the `trainFraction` too low"
        );
    }

    @Value.Derived
    @Configuration.Ignore
    default ExpectedSetSizes expectedSetSizes(long relationshipCount) {
        // division by 2 as the input is undirected but the selected relationships are directed
        long positiveTestSetSize = (long) (relationshipCount * testFraction() / 2);
        long testSetSize = (long) (positiveTestSetSize * (1 + negativeSamplingRatio()));
        long testComplementSize = (long) (relationshipCount * (1 - testFraction()));

        // division by 2 as the input is undirected but the selected relationships are directed
        long positiveTrainSetSize = (long) (testComplementSize * trainFraction() / 2);
        long trainSetSize = (long) (positiveTrainSetSize * (1 + negativeSamplingRatio()));
        long featureInputSize = (long) (testComplementSize * (1 - trainFraction()));
        long foldSize = trainSetSize / validationFolds();

        return ImmutableExpectedSetSizes.builder()
            .testSize(testSetSize)
            .trainSize(trainSetSize)
            .featureInputSize(featureInputSize)
            .testComplementSize(testComplementSize)
            .validationFoldSize(foldSize)
            .build();
    }

    @Value.Derived
    @Configuration.Ignore
    default GraphDimensions expectedGraphDimensions(GraphDimensions baseDim, String targetRelType) {
        var expectedSetSizes = expectedSetSizes(baseDim
            .relationshipCounts()
            .getOrDefault(RelationshipType.of(targetRelType), baseDim.relCountUpperBound())
        );

        return GraphDimensions.builder()
            .nodeCount(baseDim.nodeCount())
            // matches the relCount of the original GraphStore and thus lower than the sum of all relationshipCounts
            .relCountUpperBound(baseDim.relCountUpperBound())
            .putRelationshipCount(testRelationshipType(), expectedSetSizes.testSize())
            .putRelationshipCount(testComplementRelationshipType(), expectedSetSizes.testComplementSize())
            .putRelationshipCount(trainRelationshipType(), expectedSetSizes.trainSize())
            .putRelationshipCount(featureInputRelationshipType(), expectedSetSizes.featureInputSize())
            .putAllRelationshipCounts(baseDim.relationshipCounts())
            .build();
    }
}
