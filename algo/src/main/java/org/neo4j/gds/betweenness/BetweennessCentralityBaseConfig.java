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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Locale;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface BetweennessCentralityBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig {

    Optional<Long> samplingSize();

    Optional<Long> samplingSeed();

    @Configuration.Check
    default void validate() {
        samplingSize().ifPresent(samplingSize -> {
            if (samplingSize < 0) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Configuration parameter 'samplingSize' must be a positive number, got %d.",
                    samplingSize
                ));
            }
        });
    }

    @Configuration.GraphStoreValidationCheck
    default void checkNoMixedOrientations(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {

        var directionMap = graphStore.schema().relationshipSchema().directions();

        var selectedDirectionsCount = selectedRelationshipTypes.stream()
            .map(directionMap::get)
            .map(Direction::toOrientation)
            .distinct()
            .count();

        if (selectedDirectionsCount > 1) {
            throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                "Combining UNDIRECTED orientation with NATURAL or REVERSE is not supported. Found projections: %s.",
                StringJoining.join(directionMap.entrySet()
                    .stream()
                    .map(entry -> formatWithLocale(
                        "%s (%s)",
                        entry.getKey().name(),
                        entry.getValue().toOrientation().name()

                    ))
                    .sorted())
            ));
        }
    }

    @Configuration.Ignore
    default BetweennessCentralityParameters toParameters() {
        return new BetweennessCentralityParameters(
            typedConcurrency(),
            samplingSize(),
            samplingSeed(),
            hasRelationshipWeightProperty()
        );
    }
}
