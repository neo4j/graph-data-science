/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@GdlExtension
class UniformNeighborhoodSamplerTest {

    @GdlGraph
    private static final String GRAPH =
        "(x)-[]->(y)-[]->(z), (a)-[]->(b)-[]->(c), (x)-[]->(d)-[]->(e), (a)-[]->(f) , (a)-[]->(g), (a)-[]->(h)";

    @Inject
    private Graph graph;

    @Test
    void shouldSampleSubsetOfNeighbors() {

        UniformNeighborhoodSampler sampler = new UniformNeighborhoodSampler(0L);
        int numberOfSamples = 3;
        List<Long> sample = sampler.sample(graph, 3L, numberOfSamples);

        assertNotNull(sample);
        assertEquals(numberOfSamples, sample.size());
        // keep non-neighbors
        sample.retainAll(List.of(0, 1, 2, 3, 5, 6, 7));
        assertEquals(0, sample.size());

    }

    @Test
    void shouldSampleAllNeighborsWhenNumberOfSamplesAreGreater() {
        UniformNeighborhoodSampler sampler = new UniformNeighborhoodSampler(0L);
        int numberOfSamples = 19;
        List<Long> sample = sampler.sample(graph, 3L, numberOfSamples);

        assertNotNull(sample);
        assertEquals(4, sample.size());
    }
}
