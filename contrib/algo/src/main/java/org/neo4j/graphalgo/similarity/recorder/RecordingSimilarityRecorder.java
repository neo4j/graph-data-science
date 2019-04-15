/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.similarity.recorder;

import org.neo4j.graphalgo.similarity.RleDecoder;
import org.neo4j.graphalgo.similarity.SimilarityComputer;
import org.neo4j.graphalgo.similarity.SimilarityResult;

import java.util.concurrent.atomic.LongAdder;

public class RecordingSimilarityRecorder<T> implements SimilarityRecorder<T> {

    private final SimilarityComputer<T> computer;
    private final LongAdder computations = new LongAdder();

    public RecordingSimilarityRecorder(SimilarityComputer computer) {
        this.computer = computer;
    }

    public long count() {
        return computations.longValue();
    }


    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {
        computations.increment();
        return computer.similarity(decoder, source, target, cutoff);
    }
}

