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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeDoubleArray;

import java.util.function.LongToDoubleFunction;


public class PageRankResult implements CentralityAlgorithmResult {

    private final HugeDoubleArray scores;
    private final int ranIterations;
    private final boolean didConverge;

    public PageRankResult(HugeDoubleArray scores, int ranIterations, boolean didConverge) {
        this.scores = scores;
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
    }

    public int iterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    @Override
    public NodePropertyValues nodePropertyValues() {
        return NodePropertyValuesAdapter.adapt(scores);
    }

    @Override
    public LongToDoubleFunction centralityScoreProvider() {
        return scores::get;
    }
}
