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
package org.neo4j.gds.procedures.algorithms.similarity.stats;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SimilarityStatsToolsTest {

    @Test
    void shouldReturnEmptyDistributionIfNotSpecified() {

        var stats = SimilarityStatsTools.computeSimilarityStatistics(
            mock(IdMap.class),
            new Concurrency(1),
            Stream.of(new SimilarityResult(0, 1, 10)),
            false,
            TerminationFlag.RUNNING_TRUE
        );
        assertThat(stats.histogram()).isEmpty();
        assertThat(stats.success()).isTrue();
    }

    @Test
    void shouldReturnValidDistributionIfTrue(){

        var nodesBuilder = new NodesBuilderBuilder().maxOriginalId(3).build();
        nodesBuilder.addNode(0);
        nodesBuilder.addNode(1);
        nodesBuilder.addNode(2);
        nodesBuilder.addNode(3);
        var idMap = nodesBuilder.build().idMap();
        var stats = SimilarityStatsTools.computeSimilarityStatistics(
            idMap,
            new Concurrency(1),
            Stream.of(
                new SimilarityResult(0, 1, 10),
                new SimilarityResult(1,2,5),
                new SimilarityResult(1,3,9)
            ),
            true,
            TerminationFlag.RUNNING_TRUE
        );
        assertThat(stats.histogram().get().getMean()).isCloseTo(8.0, Offset.offset(1e-3));
        assertThat(stats.success()).isTrue();


    }

}
