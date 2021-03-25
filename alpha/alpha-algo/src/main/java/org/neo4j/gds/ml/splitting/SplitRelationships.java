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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.ml.splitting.EdgeSplitter.SplitResult;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;

public class SplitRelationships extends Algorithm<SplitRelationships, SplitResult> {

    private final Graph graph;
    private final Graph masterGraph;
    private final SplitRelationshipsMutateConfig config;

    public SplitRelationships(Graph graph, Graph masterGraph, SplitRelationshipsMutateConfig config) {
        this.graph = graph;
        this.masterGraph = masterGraph;
        this.config = config;
    }

    @Override
    public SplitResult compute() {
        var splitter = graph.isUndirected()
            ? new UndirectedEdgeSplitter(config.randomSeed(), config.negativeRatio())
            : new DirectedEdgeSplitter(config.randomSeed(), config.negativeRatio());
        return splitter.split(graph, masterGraph, config.holdoutFraction());
    }

    @Override
    public SplitRelationships me() {
        return this;
    }

    @Override
    public void release() {

    }
}
