/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file.csv.estimation;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

class RelationshipPropertySampler {

    private final double samplingFactor;
    private final GraphStore graphStore;
    private final ThreadLocalRandom random;

    RelationshipPropertySampler(GraphStore graphStore, double samplingFactor) {
        this.graphStore = graphStore;
        this.random = ThreadLocalRandom.current();

        this.samplingFactor = samplingFactor;
    }

    /**
     * @return The average number of relationship property characters per relationship entry.
     */
    long sample() {
        return graphStore
            .relationshipTypes()
            .stream()
            .mapToLong(relationshipType -> {
                var relativeRelTypeSize = graphStore.relationshipCount(relationshipType) / (double) graphStore.relationshipCount();
                var averageRelTypeCharacterCount = sample(relationshipType);

                return Math.round(relativeRelTypeSize * averageRelTypeCharacterCount);
            }).sum();
    }

    private int sample(RelationshipType relationshipType) {
        var propertyKeys = graphStore.relationshipPropertyKeys(relationshipType);

        if (propertyKeys.isEmpty()) {
            return 0;
        }

        var graphs = propertyKeys
            .stream()
            .map(property -> graphStore.getGraph(relationshipType, Optional.of(property)))
            .collect(Collectors.toList());

        var nodesToSample = (int) Math.round(graphs.get(0).relationshipCount() * samplingFactor);

        var propertyCharactersSamples = graphs
            .stream()
            .map((ignore) -> new ArrayList<Integer>(nodesToSample))
            .collect(Collectors.toList());


        long i = 0;
        while (i < nodesToSample) {
            var nodeId = random.nextLong(graphStore.nodeCount());

            if (graphs.get(0).degree(nodeId) == 0) {
                continue;
            }

            for (int j = 0; j < graphs.size(); j++) {
                var graph = graphs.get(j);
                var samples = propertyCharactersSamples.get(j);

                graph.forEachRelationship(nodeId, Double.NaN, (s, t, w) -> {
                    samples.add(getCharacterCount(w));
                    return false;
                });
            }

            i++;
        }

        return propertyCharactersSamples
            .stream()
            .mapToInt(samples -> samples.isEmpty() ? 0 : samples.stream().reduce(0, Integer::sum) / samples.size() + 1)
            .sum();
    }

    private int getCharacterCount(Object o) {
        return o.toString().getBytes(StandardCharsets.UTF_8).length;
    }
}

