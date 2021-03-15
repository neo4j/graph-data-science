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
package org.neo4j.gds.scaling;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.List;

/**
 * This algorithm takes as input a list of node property names and a same-sized list of scalers.
 * It applies the scalers to the node property values, respectively, and outputs a single node property.
 * The output node property values are lists of the same size as the input lists, and contain the scaled values
 * of the input node properties scaled according to the specified scalers.
 */
public class ScaleProperties extends Algorithm<ScaleProperties, ScaleProperties.Result> {

    private final Graph graph;
    private final ScalePropertiesBaseConfig config;
    private final AllocationTracker tracker;

    public ScaleProperties(Graph graph, ScalePropertiesBaseConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
    }

    @Override
    public Result compute() {
        var scaledProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount(), tracker);

        var propertyCount = config.featureProperties().size();
        initializeArrays(scaledProperties, propertyCount);

        List<Scaler> scalers = resolveScalers();
        for (int i = 0; i < propertyCount; i++) {
            scaleProperty(scaledProperties, scalers.get(i), i);
        }

        return Result.of(scaledProperties);
    }

    private void initializeArrays(HugeObjectArray<double[]> scaledProperties, int propertyCount) {
        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            config.concurrency(),
            (nodeId) -> scaledProperties.set(nodeId, new double[propertyCount])
        );
    }

    private void scaleProperty(HugeObjectArray<double[]> scaledProperties, Scaler scaler, int index) {
        ParallelUtil.parallelForEachNode(graph.nodeCount(), config.concurrency(), (nodeId) -> {
            var afterValue = scaler.scaleProperty(nodeId);
            double[] existingResult = scaledProperties.get(nodeId);
            existingResult[index] = afterValue;
        });
    }

    @Override
    public ScaleProperties me() {
        return this;
    }

    @Override
    public void release() {}

    @ValueClass
    interface Result {
        HugeObjectArray<double[]> scaledProperties();

        static Result of(HugeObjectArray<double[]> properties) {
            return ImmutableResult.of(properties);
        }
    }

    private List<Scaler> resolveScalers() {
        assert config.scalers().size() == config.featureProperties().size();

        List<Scaler> scalers = new ArrayList<>();

        for (int i = 0; i < config.scalers().size(); i++) {
            String scalerName = config.scalers().get(i);
            String property = config.featureProperties().get(i);

            var nodeProperties = graph.nodeProperties(property);
            scalers.add(Scaler.Factory.create(scalerName, nodeProperties, graph.nodeCount(), config.concurrency()));
        }
        return scalers;
    }

}
