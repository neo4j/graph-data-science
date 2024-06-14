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
import org.neo4j.gds.termination.TerminationFlag;

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
    void shouldWorkWithoutRestarts() {
        var kmeansConfig = KmeansStreamConfigImpl.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .maxIterations(1)
            .randomSeed(11L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig.toParameters(), kmeansContext, TerminationFlag.RUNNING_TRUE);
        var result = kmeans.compute();
        var communities = result.communities();
        var distances = result.distanceFromCenter();
        var averageDistance = result.averageDistanceToCentroid();
        var centers = result.centers();

        assertThat(communities.get(0)).isNotEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(1));

        assertThat(distances.get(0)).isEqualTo(0d);
        assertThat(distances.get(1)).isCloseTo(49.45, Offset.offset(1e-4));
        assertThat(distances.get(2)).isCloseTo(49.45, Offset.offset(1e-4));

        assertThat(centers[0]).isEqualTo(new double[]{1.0});
        assertThat(centers[1][0]).isCloseTo(50.55, Offset.offset(1e-4));

        assertThat(averageDistance).isCloseTo(distances.stream().sum() / 3.0, Offset.offset(1e-4));

    }

    @Test
    void shouldChangeForWithRestarts() {
        var kmeansConfig = KmeansStreamConfigImpl.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(11L)
            .maxIterations(1)
            .numberOfRestarts(2)
            .k(2)
            .build();

        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig.toParameters(), kmeansContext, TerminationFlag.RUNNING_TRUE);
        var result = kmeans.compute();
        var communities = result.communities();
        var distances = result.distanceFromCenter();
        var centers = result.centers();
        var averageDistance = result.averageDistanceToCentroid();

        assertThat(communities.get(2)).isNotEqualTo(communities.get(1));
        assertThat(communities.get(0)).isEqualTo(communities.get(1));

        assertThat(distances.get(2)).isEqualTo(0.0d);
        assertThat(distances.get(0)).isCloseTo(0.05, Offset.offset(1e-4));
        assertThat(distances.get(1)).isCloseTo(0.05, Offset.offset(1e-4));

        assertThat(centers[0][0]).isCloseTo(1.05, Offset.offset(1e-4));
        assertThat(centers[1]).isEqualTo(new double[]{100});

        assertThat(averageDistance).isCloseTo(distances.stream().sum() / 3.0, Offset.offset(1e-4));
    }



    @Test
    void shouldNotIgnoreFirstIteration() {
        //the seed forces to pick as centers:
        //in first iteration: optimally:   100.0 alone and one of the others two
        //in second iteration: 100.0 is not selected as a cluster center
        var kmeansConfig = KmeansStreamConfigImpl.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(2L)
            .maxIterations(1) //no room for improvement
            .numberOfRestarts(2)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(graph, kmeansConfig.toParameters(), kmeansContext, TerminationFlag.RUNNING_TRUE);
        var centers = kmeans.compute().centers();
        assertThat(centers[1]).isEqualTo(new double[]{100.0});
        assertThat(centers[0][0]).isCloseTo(1.05, Offset.offset(1e-4));


    }


}
