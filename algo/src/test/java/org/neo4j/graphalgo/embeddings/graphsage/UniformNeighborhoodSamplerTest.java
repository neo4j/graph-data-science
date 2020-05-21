/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.embeddings.graphsage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestGraph;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UniformNeighborhoodSamplerTest extends AlgoTestBase {

    private Graph graph;

    @BeforeEach
    void setUp() {
        graph = TestGraph.Builder.fromGdl(
            "(a)-[]->(b)-[]->(c), (a)-[]->(d) , (a)-[]->(e), (a)-[]->(f)");
    }

    @Test
    void shouldSampleSubsetOfNeighbors() {

        UniformNeighborhoodSampler sampler = new UniformNeighborhoodSampler();
        int numberOfSamples = 3;
        List<Long> sample = sampler.sample(graph, 0L, numberOfSamples);

        assertNotNull(sample);
        assertEquals(numberOfSamples, sample.size());
    }

    @Test
    void shouldSampleAllNeighborsWhenNumberOfSamplesAreGreater() {
        UniformNeighborhoodSampler sampler = new UniformNeighborhoodSampler();
        int numberOfSamples = 19;
        List<Long> sample = sampler.sample(graph, 0L, numberOfSamples);

        assertNotNull(sample);
        assertEquals(4, sample.size());
    }
}