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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NeighborhoodSamplerTest {

    @GdlGraph
    private static final String GRAPH =
        "(x)-->(y)-->(z), " +
        "(a)-->(b)-->(c), " +
        "(x)-->(d)-->(e), " +
        "(a)-->(f) , " +
        "(a)-->(g), " +
        "(a)-->(h)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldSampleSubsetOfNeighbors() {

        NeighborhoodSampler sampler = new NeighborhoodSampler(0L);
        int numberOfSamples = 3;
        List<Long> sample = sampler.sample(graph, idFunction.of("a"), numberOfSamples);

        assertThat(sample)
            .isNotNull()
            .hasSize(numberOfSamples)
            .containsAnyOf(
                idFunction.of("b"),
                idFunction.of("f"),
                idFunction.of("g"),
                idFunction.of("h")
            )
            .doesNotContain( // does not contain negative neighbors
                idFunction.of("x"),
                idFunction.of("y"),
                idFunction.of("z"),
                idFunction.of("a"),
                idFunction.of("c"),
                idFunction.of("d"),
                idFunction.of("e")
            );
    }

    @Test
    void shouldSampleAllNeighborsWhenNumberOfSamplesAreGreater() {
        NeighborhoodSampler sampler = new NeighborhoodSampler(0L);
        int numberOfSamples = 19;
        List<Long> sample = sampler.sample(graph, idFunction.of("a"), numberOfSamples);

        assertThat(sample)
            .isNotNull()
            .hasSize(4);
    }

    @Nested
    class WeightedCase {

        @GdlGraph
        private static final String GRAPH =
            "(x)-[:R { weight: 8.0 }]->(y)-[:R { weight: 4.6 }]->(z), " +
            "(a)-[:R { weight: 62.0 }]->(b)-[:R { weight: 4.6 }]->(c), " +
            "(x)-[:R { weight: 3.0 }]->(d)-[:R { weight: 4.6 }]->(e), " +
            "(a)-[:R { weight: 14.0 }]->(f), " +
            "(a)-[:R { weight: 37.0 }]->(g), " +
            "(a)-[:R { weight: 5.0 }]->(h)";

        @Test
        void shouldSampleSubsetOfNeighbors() {

            NeighborhoodSampler sampler = new NeighborhoodSampler(0L);
            int numberOfSamples = 3;
            List<Long> sample = sampler.sample(graph, idFunction.of("a"), numberOfSamples);

            assertThat(sample)
                .isNotNull()
                .hasSize(numberOfSamples)
                .containsAnyOf(
                    idFunction.of("b"),
                    idFunction.of("f"),
                    idFunction.of("g"),
                    idFunction.of("h")
                )
                .doesNotContain( // does not contain negative neighbors
                    idFunction.of("x"),
                    idFunction.of("y"),
                    idFunction.of("z"),
                    idFunction.of("a"),
                    idFunction.of("c"),
                    idFunction.of("d"),
                    idFunction.of("e")
                );
        }
    }
}
