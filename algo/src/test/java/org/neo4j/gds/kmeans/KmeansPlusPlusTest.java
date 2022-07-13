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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension

public class KmeansPlusPlusTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a1 {  kmeans: [1.0]} )" +
        "  (a2 {  kmeans: [2.0]} )" +
        "  (a3 {  kmeans: [3.0]} )" +
        "  (a4 {  kmeans: [4.0]} )" +
        "  (a5 {  kmeans: [5.0]} )" +
        "  (a6 {  kmeans: [6.0]} )" +
        "  (a7 {  kmeans: [7.0]} )" +
        "  (a8 {  kmeans: [8.0]} )" +
        "  (a9 {  kmeans: [9.0]} )" +
        "  (a10 {  kmeans: [10.0]} )";

    @Inject
    private Graph graph;


    @Inject
    private IdFunction idFunction;

    @Test
    void KmeansPlusPlusShouldWork() {
        // random.nextLong(10))  --> gives the first centroid with [9]
        // the square distances are hence [64,49,36,25,16,9,4,1,0,1] --> sum of 204
        // random.nextDouble()) --> gives 0.1166   <= 64/204 (appx 0.311) --> the second centroid is  [1]
        //the square distances are hence [0,1,4,9,16,9,4,1,0,1] --> sum of 45
        // random.nextDouble()) --> gives 0.3636
        // 1/45  : 0.022  not selected
        // 4/45  : 0.088 (0.11 total) not selected
        // 9/45  : 0.2  (0.31 total not selected)
        // 16/45 : 0.35 (0.66 total) and 0.3636 <=0.66 so last center should be [5]

        var kmeansContext = ImmutableKmeansContext.builder().build();

        var nodePropertyValues = graph.nodeProperties("kmeans");

        ClusterManager clusterManager = ClusterManager.createClusterManager(nodePropertyValues, 1, 3);
        HugeIntArray communities = HugeIntArray.newArray(10);
        HugeDoubleArray distanceFromCentroid = HugeDoubleArray.newArray(10);
        var tasks = List.of(
            KmeansTask.createTask(
                KmeansSampler.SamplerType.KMEANSPP,
                clusterManager,
                nodePropertyValues,
                communities,
                distanceFromCentroid,
                3,
                1,
                new Partition(0, 5),
                ProgressTracker.NULL_TRACKER
            ),
            KmeansTask.createTask(
                KmeansSampler.SamplerType.KMEANSPP,
                clusterManager,
                nodePropertyValues,
                communities,
                distanceFromCentroid,
                3,
                1,
                new Partition(5, 5),
                ProgressTracker.NULL_TRACKER
            )
        );
        KmeansSampler kmeansSampler = KmeansSampler.createSampler(
            KmeansSampler.SamplerType.KMEANSPP,
            new SplittableRandom(19),
            nodePropertyValues,
            clusterManager,
            10,
            3,
            2,
            distanceFromCentroid,
            kmeansContext.executor(),
            tasks
        );
        kmeansSampler.performInitialSampling();
        double[] distanceArray = new double[]{0, 1, 2, 1, 0, 1, 2, 1, 0, 1};
        assertThat(distanceFromCentroid.toArray()).isEqualTo(distanceArray);
        int[] communitiesArray = new int[]{1, 1, 1, 2, 2, 2, 0, 0, 0, 0};
        assertThat(communities.toArray()).isEqualTo(communitiesArray);


    }

}
