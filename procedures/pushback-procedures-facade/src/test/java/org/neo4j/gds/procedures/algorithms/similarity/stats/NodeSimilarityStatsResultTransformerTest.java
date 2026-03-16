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

import com.carrotsearch.hppc.BitSet;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.TopKMap;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

class NodeSimilarityStatsResultTransformerTest {

    @Test
    void shouldComputeResultWithDistribution(){

        var config = Map.of("a",(Object)("foo"));

        var bitset = new BitSet(4);
        bitset.set(0, 2);
        var topKMap = new TopKMap(4, bitset, 2, true);

        topKMap.put(0, 1, 10);
        topKMap.put(1, 2, 5);
        topKMap.put(1, 3, 9);

        var transformer = new NodeSimilarityStatsResultTransformer(
            true,
            config,
            new Concurrency(1),
            TerminationFlag.RUNNING_TRUE
        );

        var nodeSimResult = new NodeSimilarityResult(
            Optional.empty(),
            Optional.of(topKMap),
            100
        );

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(nodeSimResult,10));
        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.similarityPairs()).isEqualTo(3);
                assertThat(stats.nodesCompared()).isEqualTo(100);
                assertThat(stats.configuration()).isEqualTo(config);
                assertThat(stats.similarityDistribution())
                    .hasEntrySatisfying("mean", e-> assertThat(e)
                        .asInstanceOf(DOUBLE)
                        .isCloseTo(8, Offset.offset(1e-3)));
            });

    }

    @Test
    void shouldComputeResultWithoutDistribution(){

        var config = Map.of("a",(Object)("foo"));

        var bitset = new BitSet(4);
        bitset.set(0, 2);
        var topKMap = new TopKMap(4, bitset, 2, true);

        topKMap.put(0, 1, 10);
        topKMap.put(1, 2, 5);
        topKMap.put(1, 3, 9);

        var nodeSimResult = new NodeSimilarityResult(
            Optional.empty(),
            Optional.of(topKMap),
            100
        );

        var transformer = new NodeSimilarityStatsResultTransformer(
            false,
            config,
            new Concurrency(1),
            TerminationFlag.RUNNING_TRUE
        );


        var statsResult = transformer.apply(new TimedAlgorithmResult<>(nodeSimResult,10));
        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.similarityPairs()).isEqualTo(3);
                assertThat(stats.nodesCompared()).isEqualTo(100);
                assertThat(stats.configuration()).isEqualTo(config);
                assertThat(stats.similarityDistribution()).isEmpty();
            });

    }

    @Test
    void shouldComputeResultForEmpty(){

        var config = Map.of("a",(Object)("foo"));
        var transformer = new NodeSimilarityStatsResultTransformer(
            true,
            config,
            new Concurrency(1),
            TerminationFlag.RUNNING_TRUE
        );

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(NodeSimilarityResult.EMPTY,10));
        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.similarityPairs()).isEqualTo(0);
                assertThat(stats.nodesCompared()).isEqualTo(0);
                assertThat(stats.configuration()).isEqualTo(config);
                assertThat(stats.similarityDistribution()).isEmpty();
            });
    }

}
