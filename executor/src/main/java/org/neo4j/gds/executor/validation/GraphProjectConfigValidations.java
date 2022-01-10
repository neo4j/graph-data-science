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
package org.neo4j.gds.executor.validation;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collections;

import static java.util.stream.Collectors.toList;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphProjectConfigValidations {

    public static class UndirectedGraphValidation<CONFIG extends AlgoBaseConfig> implements BeforeLoadValidation<CONFIG> {
        @Override
        public void validateConfigsBeforeLoad(GraphProjectConfig graphProjectConfig, CONFIG config) {
            graphProjectConfig.accept(new GraphProjectConfig.Visitor() {
                @Override
                public void visit(GraphProjectFromStoreConfig storeConfig) {
                    storeConfig.relationshipProjections().projections().entrySet().stream()
                        .filter(entry -> config
                                             .relationshipTypes()
                                             .equals(Collections.singletonList(ElementProjection.PROJECT_ALL)) ||
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
    }

    /**
     * Validates that {@link Orientation#UNDIRECTED} is not mixed with {@link Orientation#NATURAL}
     * and {@link Orientation#REVERSE}. If a relationship type filter is present in the algorithm
     * config, only those relationship projections are considered in the validation.
     */
    public static class OrientationValidation<CONFIG extends AlgoBaseConfig> implements BeforeLoadValidation<CONFIG> {
        @Override
        public void validateConfigsBeforeLoad(GraphProjectConfig graphProjectConfig, CONFIG algorithmConfig) {
            graphProjectConfig.accept(new GraphProjectConfig.Visitor() {
                @Override
                public void visit(GraphProjectFromStoreConfig storeConfig) {
                    var filteredProjections = storeConfig
                        .relationshipProjections()
                        .projections()
                        .entrySet()
                        .stream()
                        .filter(entry -> algorithmConfig
                                             .relationshipTypes()
                                             .equals(Collections.singletonList(ElementProjection.PROJECT_ALL)) ||
                                         algorithmConfig.relationshipTypes().contains(entry.getKey().name()))
                        .collect(toList());

                    boolean allUndirected = filteredProjections
                        .stream()
                        .allMatch(entry -> entry.getValue().orientation() == Orientation.UNDIRECTED);

                    boolean anyUndirected = filteredProjections
                        .stream()
                        .anyMatch(entry -> entry.getValue().orientation() == Orientation.UNDIRECTED);

                    if (anyUndirected && !allUndirected) {
                        throw new IllegalArgumentException(StringFormatting.formatWithLocale(
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
    }
}
