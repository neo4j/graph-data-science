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
package org.neo4j.graphalgo.impl.node2vec;

import org.deeplearning4j.models.embeddings.learning.impl.elements.SkipGram;
import org.deeplearning4j.models.sequencevectors.interfaces.SequenceIterator;
import org.deeplearning4j.models.sequencevectors.sequence.Sequence;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Node2Vec extends Algorithm<Node2Vec, Node2Vec> {

    private static final double RETURN_PARAM = 1.0;
    private static final double IN_OUT_PARAM = 1.0;
    private Graph graph;
    private final Node2VecBaseConfig config;
    private Word2Vec word2Vec;

    public Node2Vec(Graph graph, Node2VecBaseConfig config) {
        this.graph = graph;
        this.config = config;
    }

    @Override
    public Node2Vec compute() {
        RandomWalk randomWalk = new RandomWalk(
            graph,
            config.steps(),
            new RandomWalk.NextNodeStrategy(graph, RETURN_PARAM, IN_OUT_PARAM),
            config.concurrency(),
            config.walksPerNode()
        );
        Stream<long[]> walks = randomWalk.compute();
        randomWalk.release();

        SequenceIterator<VocabWord> walkIterator = new WalkPathSentenceIterator(walks);

        word2Vec = new Word2Vec.Builder()
            .windowSize(10)
            .layerSize(config.dimensions())
            .useHierarchicSoftmax(false)
            .epochs(config.iterations())
            .elementsLearningAlgorithm(new SkipGram<>())
            .learningRate(0.01)
            .negativeSample(1)
            .iterate(walkIterator)
            .workers(config.concurrency())
            .build();

        word2Vec.fit();

        return this;
    }

    @Override
    public Node2Vec me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    public double[] embeddingForNode(long nodeId) {
        return word2Vec.getWordVector(Long.toString(nodeId));
    }

    static class WalkPathSentenceIterator implements SequenceIterator<VocabWord> {

        private final AtomicInteger sentenceCounter;
        private final Collection<long[]> allPaths;

        private Iterator<long[]> iterator;

        WalkPathSentenceIterator(Stream<long[]> allPathsStream) {
            this.allPaths = allPathsStream.collect(Collectors.toList());
            iterator = allPaths.iterator();
            this.sentenceCounter = new AtomicInteger(0);
        }

        @Override
        public boolean hasMoreSequences() {
            return iterator.hasNext();
        }

        @Override
        public Sequence<VocabWord> nextSequence() {
            long[] path = iterator.next();
            var sequence = new Sequence<VocabWord>();
            sequence.setSequenceId(sentenceCounter.getAndIncrement());

            for(var nodeId: path) {
                sequence.addElement(new VocabWord(1.0, Long.toString(nodeId)));
            }

            return sequence;
        }

        @Override
        public void reset() {
            iterator = allPaths.iterator();
        }
    }

}
