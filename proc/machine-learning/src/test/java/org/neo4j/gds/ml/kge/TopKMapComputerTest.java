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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class TopKMapComputerTest {

    @Test
    void shouldComputeTopKMap() {
        var sourceNodes = create(1, 3, 5);
        var targetNodes = create(4, 5, 6);
        var topK = 1;
        var concurrency = 1;

        LinkScorerFactory linkScorerFactory = TestLinkScorer::new;

        var computer = new TopKMapComputer(
            null, //TODO add graph
            sourceNodes,
            targetNodes,
            linkScorerFactory,
            (a, b) -> a != b,
            topK,
            concurrency,
            ProgressTracker.NULL_TRACKER
            );

        KGEPredictResult result = computer.compute();

        assertThat(result.topKMap().stream()).containsExactly(
            // TODO dont return a SimilarityResult here
            new SimilarityResult(1, 6, 7),
            new SimilarityResult(3, 6, 9),
            new SimilarityResult(5, 6, 11)
        );
    }

    private BitSet create(long... ids) {
        long capacity = Arrays.stream(ids).max().orElse(0);

        BitSet bitSet = new BitSet(capacity+1);

        for (long id : ids) {
            bitSet.set(id);
        }

        return bitSet;
    }

    private class TestLinkScorer implements LinkScorer {

        long currentSourceNode = 0;

        @Override
        public void init(NodePropertyValues embeddings, long sourceNode) {
           currentSourceNode = sourceNode;
        }

        @Override
        public double similarity(long targetNode) {
            return currentSourceNode + targetNode;
        }

        @Override
        public void close() throws Exception {

        }
    }

}