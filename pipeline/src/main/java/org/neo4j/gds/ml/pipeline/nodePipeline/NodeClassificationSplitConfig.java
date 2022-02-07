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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
public interface NodeClassificationSplitConfig extends ToMapConvertible {
    NodeClassificationSplitConfig DEFAULT_CONFIG = NodeClassificationSplitConfig.of(CypherMapWrapper.empty());
    int MIN_NUM_NODES_PER_SET = 1;
    int MIN_NUM_NODES_PER_TRAIN_SET = 2;//At least 2 since this will be further split during cross validation

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

    static NodeClassificationSplitConfig of(CypherMapWrapper config) {
        return new NodeClassificationSplitConfigImpl(config);
    }

    @TestOnly
    static ImmutableNodeClassificationSplitConfig.Builder testBuilder() {
        return ImmutableNodeClassificationSplitConfig.builder();
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
    default void validateMinNumNodesInSplitSets(
        Graph graph
    ) {
        long numberNodesInTestSet = (long) (graph.nodeCount() * testFraction());
        long numberNodesInTrainSet = graph.nodeCount() - numberNodesInTestSet;
        long numberNodesInValidationSet = numberNodesInTrainSet / validationFolds();

        validateNumberOfNodesInSplitSet(numberNodesInTestSet, MIN_NUM_NODES_PER_SET, "test", "testFraction");
        validateNumberOfNodesInSplitSet(numberNodesInTrainSet, MIN_NUM_NODES_PER_TRAIN_SET, "train", "testFraction");
        validateNumberOfNodesInSplitSet(numberNodesInValidationSet, MIN_NUM_NODES_PER_SET, "validation", "validationFolds");
    }

    @Configuration.Ignore
    @Value.Derived
    private static void validateNumberOfNodesInSplitSet(
        long numberNodesInSet,
        long minNumberNodes,
        String setName,
        String parameterName
    ) {
        if (numberNodesInSet < minNumberNodes) {
            throw new IllegalArgumentException(formatWithLocale(
                "The specified `%s` is not compatible with the current graph. " +
                "The %s set would have %d node(s) " +
                "but it should have at least %d. ",
                parameterName, setName, numberNodesInSet, minNumberNodes
            ));
        }
    }
}
