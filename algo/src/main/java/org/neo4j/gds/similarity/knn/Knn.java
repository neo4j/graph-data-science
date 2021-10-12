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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;

import java.util.SplittableRandom;

public class Knn extends Algorithm<Knn, RandomNeighborSamplingSimilarityComputer.Result> {

    private final long nodeCount;
    private final KnnBaseConfig config;
    private final KnnContext context;
    private final SplittableRandom random;
    private final SimilarityComputer computer;
    private final RandomNeighborSamplingSimilarityComputer similarityComputer;

    public Knn(Graph graph, KnnBaseConfig config, KnnContext context) {
        this(
            graph.nodeCount(),
            config,
            SimilarityComputer.ofProperty(graph, config.nodeWeightProperty()),
            context
        );
    }

    public Knn(
        long nodeCount,
        KnnBaseConfig config,
        SimilarityComputer similarityComputer,
        KnnContext context
    ) {
        super(context.progressTracker());
        this.nodeCount = nodeCount;
        this.config = config;
        this.context = context;
        this.computer = similarityComputer;

        this.random = this.config.randomSeed().isPresent()
            ? new SplittableRandom(this.config.randomSeed().get())
            : new SplittableRandom();
        this.similarityComputer = new RandomNeighborSamplingSimilarityComputer(
            config,
            random,
            similarityComputer,
            nodeCount,
            context.executor(),
            context.progressTracker(),
            context.allocationTracker()
        );
    }

    public long nodeCount() {
        return this.nodeCount;
    }

    public KnnContext context() {
        return context;
    }

    @Override
    public RandomNeighborSamplingSimilarityComputer.Result compute() {
        return this.similarityComputer.compute();
    }

    @Override
    public Knn me() {
        return this;
    }

    @Override
    public void release() {

    }
}
