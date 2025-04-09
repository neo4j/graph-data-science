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
package org.neo4j.gds.undirected;

import org.neo4j.gds.core.Aggregation;

import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ToUndirectedAggregationsParser {

    private ToUndirectedAggregationsParser() {}

    public static ToUndirectedAggregations parse(Object input) {
        if (input instanceof ToUndirectedAggregations toUndirectedAggregations) {
            return toUndirectedAggregations;
        }

        if (input instanceof Aggregation aggregation) {
            return new ToUndirectedAggregations.GlobalAggregation(aggregation);
        }

        if (input instanceof String) {
            return new ToUndirectedAggregations.GlobalAggregation(Aggregation.parse(input));
        }

        if (input instanceof Map) {
            var mapInput = (Map<String, Object>) input;

            Map<String, Aggregation> parsedInput = mapInput
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Aggregation.parse(entry.getValue())));

            return new ToUndirectedAggregations.AggregationPerProperty(parsedInput);
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected Map or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(ToUndirectedAggregations aggregations) {
        return aggregations.toString();
    }

}
