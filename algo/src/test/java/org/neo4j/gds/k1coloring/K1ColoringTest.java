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
package org.neo4j.gds.k1coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

class K1ColoringTest {

    @Test
    void testK1Coloring() {
        final String DB_CYPHER =
            "CREATE" +
            " (a)" +
            ",(b)" +
            ",(c)" +
            ",(d)" +
            ",(a)-[:REL]->(b)" +
            ",(a)-[:REL]->(c)";

        var graph = fromGdl(DB_CYPHER);

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            1000,
            DEFAULT_BATCH_SIZE,
            new Concurrency(1),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var result = k1Coloring.compute();

        var colors = result.colors();

        var colorOfNode0 = colors.get(0);
        var colorOfNode1 = colors.get(1);
        var colorOfNode2 = colors.get(2);

        assertThat(colorOfNode0)
            .as("Color of Node0 should be unique")
            .isNotEqualTo(colorOfNode1)
            .isNotEqualTo(colorOfNode2);
        assertThat(colorOfNode1)
            .as("Color of Node1 should be the same as color of Node2")
            .isEqualTo(colorOfNode2);

        var usedColors=result.usedColors();
        assertThat(usedColors.get(colorOfNode0)).isTrue();
        assertThat(usedColors.get(colorOfNode1)).isTrue();
        assertThat(usedColors.cardinality()).isEqualTo(2);
    }

    @Test
    void testParallelK1Coloring() {
        long seed = 42L;

        var graph = RandomGraphGenerator.builder()
            .nodeCount(200_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(seed)
            .aggregation(Aggregation.NONE)
            .direction(Direction.UNDIRECTED)
            .allowSelfLoops(AllowSelfLoops.NO)
            .build()
            .generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            new Concurrency(8),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var result = k1Coloring.compute();
        var colors = result.colors();
        var usedColors=result.usedColors();
        var conflicts = new MutableLong(0);
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                if (source != target && colors.get(source) == colors.get(target)) {
                    conflicts.increment();
                }
                return true;
            });
            return true;
        });

        assertThat(conflicts.longValue())
            .as("Conflicts should be less than 20")
            .isLessThan(20L);

        assertThat(usedColors.cardinality())
            .as("Used colors should be less than or equal to 21")
            .isLessThanOrEqualTo(21);
    }

    @Test
    void everyNodeShouldHaveBeenColored() {
        RandomGraphGenerator generator = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .build();

        Graph graph = generator.generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            new Concurrency(8),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

      var result =  k1Coloring.compute();

        assertThat(result.usedColors().get(ColoringStep.INITIAL_FORBIDDEN_COLORS))
            .as("The result should not contain the initial forbidden colors")
            .isFalse();
    }

}
