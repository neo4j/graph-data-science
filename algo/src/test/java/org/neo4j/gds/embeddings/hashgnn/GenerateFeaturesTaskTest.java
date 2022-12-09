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
package org.neo4j.gds.embeddings.hashgnn;

import org.apache.commons.lang3.mutable.MutableLong;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GenerateFeaturesTaskTest {

    @Test
    void shouldGenerateCorrectNumberOfFeatures() {
        int embeddingDimension = 100;
        int densityLevel = 8;

        Graph graph = RandomGraphGenerator.builder()
            .nodeCount(1000)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(42L)
            .build()
            .generate();

        var partition = new Partition(0, graph.nodeCount());
        var totalFeatureCount = new MutableLong(0);
        var config = HashGNNStreamConfigImpl
            .builder()
            .generateFeatures(Map.of("dimension", embeddingDimension, "densityLevel", densityLevel))
            .iterations(1337)
            .embeddingDensity(1337)
            .randomSeed(42L)
            .build();

        var output = GenerateFeaturesTask.compute(
            graph,
            List.of(partition),
            config,
            42L,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            totalFeatureCount
        );

        assertThat(output.size()).isEqualTo(graph.nodeCount());
        assertThat(totalFeatureCount.getValue()).isCloseTo(
            densityLevel * graph.nodeCount(),
            Percentage.withPercentage(10)
        );

        for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            var features = output.get(nodeId);
            assertThat(features.size()).isEqualTo(embeddingDimension);
            assertThat(features.cardinality()).isGreaterThanOrEqualTo(1);
            assertThat(features.cardinality()).isLessThanOrEqualTo(densityLevel);
        }
    }

}
