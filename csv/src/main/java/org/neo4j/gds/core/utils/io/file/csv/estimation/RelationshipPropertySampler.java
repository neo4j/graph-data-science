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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Calculates the average number of relationship property characters by sampling.
 * This is done per relationship type.
 * For each type a number of nodes with at least one relationship of the given type are randomly chosen.
 * The first relationship for each sampled node is used to estimate the property values.
 */
final class RelationshipPropertySampler {

    private final double samplingFactor;
    private final GraphStore graphStore;
    private final ThreadLocalRandom random;

    public static long sample(GraphStore graphStore, double samplingFactor) {
        return new RelationshipPropertySampler(graphStore, samplingFactor).sample();
    }

    private RelationshipPropertySampler(GraphStore graphStore, double samplingFactor) {
        this.graphStore = graphStore;
        this.random = ThreadLocalRandom.current();

        this.samplingFactor = samplingFactor;
    }

    /**
     * @return The average number of relationship property characters per relationship entry.
     */
    private long sample() {
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

        var relationshipsToSample = (int) Math.round(graphs.get(0).relationshipCount() * samplingFactor);

        var propertyCharactersSamples = graphs
            .stream()
            .map((ignore) -> new ArrayList<Integer>(relationshipsToSample))
            .collect(Collectors.toList());

        long relationshipsSampled = 0;
        while (relationshipsSampled < relationshipsToSample) {
            var nodeId = random.nextLong(graphStore.nodeCount());

            if (graphs.get(0).degree(nodeId) == 0) {
                continue;
            }

            for (int graphIndex = 0; graphIndex < graphs.size(); graphIndex++) {
                var graph = graphs.get(graphIndex);
                var samples = propertyCharactersSamples.get(graphIndex);

                graph.forEachRelationship(nodeId, Double.NaN, (s, t, w) -> {
                    samples.add(getCharacterCount(w));
                    return false;
                });
            }

            relationshipsSampled++;
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

