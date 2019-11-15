/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.jaccard;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.api.Graph;

import java.util.Optional;

public class SimilarityGraphResult {
    private final Graph similarityGraph;

    private final Optional<DoubleHistogram> maybeHistogram;
    private final long comparedNodes;

    public SimilarityGraphResult(Graph similarityGraph, Optional<DoubleHistogram> histogram, long comparedNodes) {
        this.similarityGraph = similarityGraph;
        this.maybeHistogram = histogram;
        this.comparedNodes = comparedNodes;
    }

    public Graph similarityGraph() {
        return similarityGraph;
    }

    public Optional<DoubleHistogram> histogram() {
        return maybeHistogram;
    }

    public long comparedNodes() {
        return comparedNodes;
    }
}
