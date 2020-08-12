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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.stream.DoubleStream;

import static java.util.stream.Collectors.toList;

public abstract class GraphSageAlgoBase<ALGO extends GraphSageAlgoBase<ALGO, RESULT, CONFIG>, RESULT, CONFIG extends GraphSageBaseConfig>
    extends Algorithm<ALGO, RESULT> {

    public static final String ALGO_TYPE = "graphSage";

    protected final Graph graph;
    protected final List<NodeProperties> nodeProperties;
    private final boolean useDegreeAsProperty;

    GraphSageAlgoBase(Graph graph, CONFIG config) {
        this.useDegreeAsProperty = config.degreeAsProperty();
        this.graph = graph;

        nodeProperties = config
            .nodePropertyNames()
            .stream()
            .map(graph::nodeProperties)
            .collect(toList());
    }

    protected HugeObjectArray<double[]> initializeFeatures() {
        HugeObjectArray<double[]> features = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            AllocationTracker.EMPTY
        );
        features.setAll(n -> {
            DoubleStream nodeFeatures = this.nodeProperties.stream().mapToDouble(p -> p.doubleValue(n));
            if (useDegreeAsProperty) {
                nodeFeatures = DoubleStream.concat(nodeFeatures, DoubleStream.of(graph.degree(n)));
            }
            return nodeFeatures.toArray();
        });
        return features;
    }

}
