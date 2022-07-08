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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.MIN_TRAIN_SET_SIZE;
import static org.neo4j.gds.ml.pipeline.NonEmptySetValidation.validateNodeSetSize;

@Configuration
public interface NodePropertyPredictionSplitConfig extends ToMapConvertible {
    NodePropertyPredictionSplitConfig DEFAULT_CONFIG = NodePropertyPredictionSplitConfig.of(CypherMapWrapper.empty());

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double testFraction() {
        return 0.3;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int validationFolds() {
        return 3;
    }

    static NodePropertyPredictionSplitConfig of(CypherMapWrapper config) {
        return new NodePropertyPredictionSplitConfigImpl(config);
    }

    @Override
    @Configuration.ToMap
    Map<String, Object> toMap();

    @Configuration.CollectKeys
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    @Value.Derived
    @Configuration.Ignore
    default void validateMinNumNodesInSplitSets(Graph graph) {
        long numberNodesInTestSet = (long) (graph.nodeCount() * testFraction());
        long numberNodesInTrainSet = graph.nodeCount() - numberNodesInTestSet;
        long numberNodesInValidationSet = numberNodesInTrainSet / validationFolds();

        validateNodeSetSize(numberNodesInTestSet, MIN_SET_SIZE, "test", "`testFraction` is too low");
        validateNodeSetSize(numberNodesInTrainSet, MIN_TRAIN_SET_SIZE, "train", "`testFraction` is too high");
        validateNodeSetSize(numberNodesInValidationSet, MIN_SET_SIZE, "validation", "`validationFolds` or `testFraction` is too high");
    }

    @Value.Auxiliary
    @Value.Derived
    @Configuration.Ignore
    default long testSetSize(long nodeCount) {
        return (long) (testFraction() * nodeCount);
    }

    @Value.Auxiliary
    @Value.Derived
    @Configuration.Ignore
    default long trainSetSize(long nodeCount) {
        return (long) (nodeCount * (1 - testFraction()));
    }

    @Value.Auxiliary
    @Value.Derived
    @Configuration.Ignore
    default long foldTrainSetSize(long nodeCount) {
        return trainSetSize(nodeCount) * (validationFolds() - 1) / validationFolds();
    }

    @Value.Auxiliary
    @Value.Derived
    @Configuration.Ignore
    default long foldTestSetSize(long nodeCount) {
        return trainSetSize(nodeCount) * (1 / validationFolds());
    }
}
