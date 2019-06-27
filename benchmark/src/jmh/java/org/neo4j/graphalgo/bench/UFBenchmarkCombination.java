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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphalgo.impl.unionfind.GraphUnionFindAlgo;
import org.neo4j.graphalgo.impl.unionfind.UnionFindAlgo;

import java.util.Optional;

public enum UFBenchmarkCombination {

    HEAVY_QUEUE(GraphImpl.HEAVY, UnionFindAlgo.QUEUE),
    HEAVY_FORK_JOIN(GraphImpl.HEAVY, UnionFindAlgo.FORK_JOIN),
    HEAVY_FJ_MERGE(GraphImpl.HEAVY, UnionFindAlgo.FJ_MERGE),
    HEAVY_SEQ(GraphImpl.HEAVY, UnionFindAlgo.SEQ),

    HUGE_QUEUE(GraphImpl.HUGE, UnionFindAlgo.QUEUE),
    HUGE_FORK_JOIN(GraphImpl.HUGE, UnionFindAlgo.FORK_JOIN),
    HUGE_FJ_MERGE(GraphImpl.HUGE, UnionFindAlgo.FJ_MERGE),
    HUGE_SEQ(GraphImpl.HUGE, UnionFindAlgo.SEQ);

    final GraphImpl graph;
    final UnionFindAlgo algo;

    UFBenchmarkCombination(GraphImpl graph, UnionFindAlgo algo) {
        this.graph = graph;
        this.algo = algo;
    }

    public Object run(Graph graph) {
        double threshold = Double.NaN;
        GraphUnionFindAlgo<?> unionFindAlgo = algo.algo(
                Optional.of(graph),
                Pools.DEFAULT,
                (int) (graph.nodeCount() / Pools.DEFAULT_CONCURRENCY),
                Pools.DEFAULT_CONCURRENCY,
                threshold,
                AllocationTracker.EMPTY
        );
        PagedDisjointSetStruct communities = unionFindAlgo.compute();
        unionFindAlgo.release();
        return communities;
    }
}
