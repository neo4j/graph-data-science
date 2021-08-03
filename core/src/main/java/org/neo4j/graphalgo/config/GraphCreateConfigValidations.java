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
package org.neo4j.graphalgo.config;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.utils.StringJoining;

import java.util.Collections;

import static java.util.stream.Collectors.toList;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;

public final class GraphCreateConfigValidations {

    public static <CONFIG extends AlgoBaseConfig> void validateIsUndirectedGraph(
        GraphCreateConfig graphCreateConfig,
        CONFIG config
    ) {
        graphCreateConfig.accept(new GraphCreateConfig.Visitor() {
            @Override
            public void visit(GraphCreateFromStoreConfig storeConfig) {
                storeConfig.relationshipProjections().projections().entrySet().stream()
                    .filter(entry -> config.relationshipTypes().equals(Collections.singletonList(PROJECT_ALL)) ||
                                     config.relationshipTypes().contains(entry.getKey().name()))
                    .filter(entry -> entry.getValue().orientation() != Orientation.UNDIRECTED)
                    .forEach(entry -> {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses orientation `%s`",
                            entry.getKey().name,
                            entry.getValue().orientation()
                        ));
                    });

            }
        });
    }

    /**
     * Validates that {@link Orientation#UNDIRECTED} is not mixed with {@link Orientation#NATURAL}
     * and {@link Orientation#REVERSE}. If a relationship type filter is present in the algorithm
     * config, only those relationship projections are considered in the validation.
     */
    public static <CONFIG extends AlgoBaseConfig> void validateOrientationCombinations(
        GraphCreateConfig graphCreateConfig,
        CONFIG algorithmConfig
    ) {
        graphCreateConfig.accept(new GraphCreateConfig.Visitor() {
            @Override
            public void visit(GraphCreateFromStoreConfig storeConfig) {
                var filteredProjections = storeConfig
                    .relationshipProjections()
                    .projections()
                    .entrySet()
                    .stream()
                    .filter(entry -> algorithmConfig
                                         .relationshipTypes()
                                         .equals(Collections.singletonList(PROJECT_ALL)) ||
                                     algorithmConfig.relationshipTypes().contains(entry.getKey().name()))
                    .collect(toList());

                boolean allUndirected = filteredProjections
                    .stream()
                    .allMatch(entry -> entry.getValue().orientation() == Orientation.UNDIRECTED);

                boolean anyUndirected = filteredProjections
                    .stream()
                    .anyMatch(entry -> entry.getValue().orientation() == Orientation.UNDIRECTED);

                if (anyUndirected && !allUndirected) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Combining UNDIRECTED orientation with NATURAL or REVERSE is not supported. Found projections: %s.",
                        StringJoining.join(filteredProjections
                            .stream()
                            .map(entry -> formatWithLocale(
                                "%s (%s)",
                                entry.getKey().name,
                                entry.getValue().orientation()
                            ))
                            .sorted())
                    ));
                }
            }
        });
    }

    private GraphCreateConfigValidations() {}
}
