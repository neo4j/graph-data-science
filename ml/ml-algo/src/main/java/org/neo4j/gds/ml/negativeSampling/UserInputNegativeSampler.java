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
package org.neo4j.gds.ml.negativeSampling;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Collection;
import java.util.Optional;
import java.util.SplittableRandom;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class UserInputNegativeSampler implements NegativeSampler {
    private final Graph negativeExampleGraph;
    private final double testTrainFraction;
    private final SplittableRandom rng;

    public UserInputNegativeSampler(
        Graph negativeExampleGraph,
        double testTrainFraction,
        Optional<Long> randomSeed,
        Collection<NodeLabel> sourceLabels,
        Collection<NodeLabel> targetLabels
        ) {
        if (!negativeExampleGraph.schema().isUndirected()) {
            throw new IllegalArgumentException("UserInputNegativeSampler requires graph to be UNDIRECTED.");
        }
        this.negativeExampleGraph = negativeExampleGraph;
        this.testTrainFraction = testTrainFraction;
        this.rng = randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);

        validateNegativeRelationships(sourceLabels, targetLabels);
    }

    @Override
    public void produceNegativeSamples(
        RelationshipsBuilder testSetBuilder,
        RelationshipsBuilder trainSetBuilder
    ) {
        var totalRelationshipCount = negativeExampleGraph.relationshipCount()/2;
        var testRelationshipCount = (long) (totalRelationshipCount * testTrainFraction);
        var testRelationshipsToAdd = new MutableLong(testRelationshipCount);
        var trainRelationshipsToAdd = new MutableLong(totalRelationshipCount - testRelationshipCount);

        negativeExampleGraph.forEachNode(nodeId -> {
            negativeExampleGraph.forEachRelationship(nodeId, (s, t) -> {
                if (s < t) {
                    if (sample(testRelationshipsToAdd.doubleValue()/(testRelationshipsToAdd.doubleValue() + trainRelationshipsToAdd.doubleValue()))) {
                        testRelationshipsToAdd.decrement();
                        testSetBuilder.add(s, t, NEGATIVE);
                    } else {
                        trainRelationshipsToAdd.decrement();
                        trainSetBuilder.add(s, t, NEGATIVE);
                    }
                }
                return true;
            });
            return true;
        });
    }

    private boolean sample(double probability) {
        return rng.nextDouble() < probability;
    }

    private void validateNegativeRelationships(Collection<NodeLabel> validSourceLabel, Collection<NodeLabel> validTargetLabel) {
         negativeExampleGraph.forEachNode(nodeId -> {
                negativeExampleGraph.forEachRelationship(nodeId, (s, t) -> {
                    var negativeRelHasCorrectType = nodePairsHaveValidLabels(negativeExampleGraph.nodeLabels(s), negativeExampleGraph.nodeLabels(t), validSourceLabel, validTargetLabel);
                    if (!negativeRelHasCorrectType) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "There is a relationship of negativeRelationshipType between nodes %s and %s. The nodes have types %s and %s. However, they need to be between %s and %s.",
                            negativeExampleGraph.toOriginalNodeId(s), negativeExampleGraph.toOriginalNodeId(t),
                            negativeExampleGraph.nodeLabels(s), negativeExampleGraph.nodeLabels(t), validSourceLabel.toString(), validTargetLabel.toString()
                        ));
                    }
                    return true;
                });
                return true;
            });
    }

    private boolean nodePairsHaveValidLabels(Collection<NodeLabel> candidateSource, Collection<NodeLabel> candidateTarget, Collection<NodeLabel> validSourceLabels, Collection<NodeLabel> validTargetLabels) {
        return (candidateSource.stream().anyMatch(validSourceLabels::contains)
                && candidateTarget.stream().anyMatch(validTargetLabels::contains)) ||
               ((candidateSource.stream().anyMatch(validTargetLabels::contains)
                 && candidateTarget.stream().anyMatch(validSourceLabels::contains)));
    }

}
