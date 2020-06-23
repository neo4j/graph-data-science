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
package org.neo4j.gds.embeddings.graphsage.batch;

import org.neo4j.graphalgo.api.Graph;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MiniBatchProvider implements BatchProvider {
    private final int batchSize;

    public MiniBatchProvider(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public Stream<List<Long>> stream(Graph graph) {
        final AtomicLong counter = new AtomicLong();

        return LongStream
            .range(0, graph.nodeCount())
            .boxed()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / batchSize))
            .values()
            .stream();
    }
}
