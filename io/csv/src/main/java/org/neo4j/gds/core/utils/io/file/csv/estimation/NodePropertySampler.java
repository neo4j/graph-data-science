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
package org.neo4j.gds.core.utils.io.file.csv.estimation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Calculates the average number of node property characters by sampling nodes and their properties.
 * A number of nodes is randomly selected and then classified according to their node labels.
 */
final class NodePropertySampler {

    /**
     * @return The average number of property characters per node entry.
     */
    public static long sample(GraphStore graphStore, double samplingFactor) {
        return new NodePropertySampler(graphStore, samplingFactor).sample();
    }

    private final long nodesToSample;
    private final GraphStore graphStore;
    private final Map<List<NodeLabel>, NodeLabelSample> schemaSamples;

    private NodePropertySampler(GraphStore graphStore, double samplingFactor) {
        this.graphStore = graphStore;

        this.nodesToSample = Math.round(graphStore.nodeCount() * samplingFactor);
        this.schemaSamples = new HashMap<>();
    }

    private long sample() {
        var random = ThreadLocalRandom.current();
        for (long i = 0; i < nodesToSample; i++) {
            var nodeId = random.nextLong(graphStore.nodeCount());
            var labels = graphStore.nodes().nodeLabels(nodeId);

            var schemaSample = schemaSamples.computeIfAbsent(labels, (l) -> new NodeLabelSample(graphStore, l));
            schemaSample.sample(nodeId);
        }

        return schemaSamples
            .values()
            .stream()
            .mapToLong(nodeLabelSample -> {
                var relativeSampleSize = nodeLabelSample.encounters() / (double) nodesToSample;
                var averageEntrySize = nodeLabelSample.averageEntrySize();
                return Math.round(relativeSampleSize * averageEntrySize);
            }).sum();
    }

    /**
     * Stores the sample data for a specific NodeLabel combination.
     */
    private static class NodeLabelSample {

        private final GraphStore graphStore;
        private final Map<String, List<Integer>> propertyCharactersSamples;

        private long encounters;

        NodeLabelSample(GraphStore graphStore, Collection<NodeLabel> labels) {
            this.graphStore = graphStore;
            this.propertyCharactersSamples = new HashMap<>();

            graphStore
                .nodePropertyKeys(labels)
                .forEach(propertyKey -> propertyCharactersSamples.put(propertyKey, new ArrayList<>()));
        }

        long encounters() {
            return encounters;
        }

        int averageEntrySize() {
            return propertyCharactersSamples
                .values()
                .stream()
                .mapToInt(propertySamples ->
                    propertySamples
                        .stream()
                        .reduce(0, Integer::sum) / propertySamples.size() + 1)
                .sum();
        }

        void sample(long nodeId) {
            encounters++;

            propertyCharactersSamples.forEach((propertyKey, sampleList) -> {
                var propertyValue = graphStore
                    .nodeProperty(propertyKey)
                    .values()
                    .getObject(nodeId);

                int characterCount = 0;

                if (propertyValue == null) {
                    // nothing to do here
                } else if (propertyValue.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(propertyValue); i++) {
                        characterCount += getCharacterCount(Array.get(propertyValue, i)) + 1;
                    }
                } else {
                    characterCount = getCharacterCount(propertyValue);
                }

                sampleList.add(characterCount);
            });
        }

        private int getCharacterCount(Object o) {
            return o.toString().getBytes(StandardCharsets.UTF_8).length;
        }
    }

}

