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
package org.neo4j.gds.kmeans;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class KmeansRestartsTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a {  kmeans: [1.0]} )" +
        "  (b {  kmeans: [1.1]} )" +
        "  (c {  kmeans: [100.0]} )";


    @Inject
    private Graph graph;


    @Inject
    private IdFunction idFunction;

    @Test
    void shouldChangeWithRestarts() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .maxIterations(1)
            .randomSeed(11L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var result1 = kmeans.compute();
        var communities1 = result1.communities();
        var distances1 = result1.distanceFromCenter();

        var centers1 = result1.centers();
        assertThat(communities1.get(0)).isNotEqualTo(communities1.get(1));
        assertThat(communities1.get(2)).isEqualTo(communities1.get(1));

        assertThat(distances1.get(0)).isEqualTo(0d);
        assertThat(distances1.get(1)).isCloseTo(49.45, Offset.offset(1e-4));
        assertThat(distances1.get(2)).isCloseTo(49.45, Offset.offset(1e-4));

        assertThat(centers1[0]).isEqualTo(new double[]{1.0});
        assertThat(centers1[1][0]).isCloseTo(50.55, Offset.offset(1e-4));


        var kmeansConfig2 = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(11L)
            .maxIterations(1)
            .numberOfRestarts(2)
            .k(2)
            .build();

        var kmeans2 = Kmeans.createKmeans(graph, kmeansConfig2, kmeansContext);
        var result2 = kmeans.compute();
        var communities2 = result2.communities();
        var distances2 = result2.distanceFromCenter();
        var centers2 = result2.centers();

        assertThat(communities2.get(2)).isNotEqualTo(communities2.get(1));
        assertThat(communities2.get(0)).isEqualTo(communities2.get(1));

        assertThat(distances2.get(2)).isEqualTo(0.0d);
        assertThat(distances2.get(0)).isCloseTo(0.05, Offset.offset(1e-4));
        assertThat(distances2.get(1)).isCloseTo(0.05, Offset.offset(1e-4));

        assertThat(centers2[0][0]).isCloseTo(1.05, Offset.offset(1e-4));
        assertThat(centers2[1]).isEqualTo(new double[]{100});

    }


    @Test
    void shouldNotIgnoreFirstIteration() {

        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(2L)
            .maxIterations(1)
            .numberOfRestarts(2)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
        var centers = kmeans.compute().centers();
        assertThat(centers[1]).isEqualTo(new double[]{100.0});
        assertThat(centers[0][0]).isCloseTo(1.05, Offset.offset(1e-4));


    }


}
