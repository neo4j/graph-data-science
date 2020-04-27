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
package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;

import java.util.Collections;

import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;

final class ValidationUtil {

    private ValidationUtil() {}

    static <CONFIG extends AlgoBaseConfig> void validateConfigs(GraphCreateConfig graphCreateConfig, CONFIG config) {
        if (!graphCreateConfig.isCypher()) {
            GraphCreateFromStoreConfig storeConfig = (GraphCreateFromStoreConfig) graphCreateConfig;
            storeConfig.relationshipProjections().projections().entrySet().stream()
                .filter(entry -> config.relationshipTypes().equals(Collections.singletonList(PROJECT_ALL)) ||
                                 config.relationshipTypes().contains(entry.getKey().name()))
                .filter(entry -> entry.getValue().orientation() != Orientation.UNDIRECTED)
                .forEach(entry -> {
                    throw new IllegalArgumentException(String.format(
                        "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses orientation `%s`",
                        entry.getKey().name,
                        entry.getValue().orientation()
                    ));
                });
        }
    }

}
