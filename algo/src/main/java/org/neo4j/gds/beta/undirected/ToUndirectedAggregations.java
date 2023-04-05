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
package org.neo4j.gds.beta.undirected;

import org.neo4j.gds.core.Aggregation;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class ToUndirectedAggregations {

    abstract Set<String> propertyKeys();

    abstract Aggregation localAggregation(String propertyKey);

    abstract Optional<Aggregation> globalAggregation();

    public static ToUndirectedAggregations of(Object input) {
        if (input instanceof ToUndirectedAggregations) {
            return (ToUndirectedAggregations) input;
        }

        if (input instanceof Aggregation) {
            return new GlobalAggregation((Aggregation) input);
        }

        if (input instanceof String) {
            return new GlobalAggregation(Aggregation.parse(input));
        }

        if (input instanceof Map) {
            var mapInput = (Map<String, Object>) input;

            Map<String, Aggregation> parsedInput = mapInput
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Aggregation.parse(entry.getValue())));

            return new AggregationPerProperty(parsedInput);
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected Map or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(ToUndirectedAggregations aggregations) {
        return aggregations.toString();
    }

    private static final class GlobalAggregation extends ToUndirectedAggregations {
       private final Aggregation aggregation;

        private GlobalAggregation(Aggregation aggregation) {this.aggregation = aggregation;}

        @Override
        Set<String> propertyKeys() {
            return Set.of();
        }

        @Override
        Aggregation localAggregation(String propertyKey) {
            return aggregation;
        }

        @Override
        Optional<Aggregation> globalAggregation() {
            return Optional.of(aggregation);
        }

        @Override
        public String toString() {
            return aggregation.toString();
        }
    }


    private static final class AggregationPerProperty extends ToUndirectedAggregations {
        private final Map<String, Aggregation> aggregations;

        private AggregationPerProperty(Map<String, Aggregation> aggregations) {this.aggregations = aggregations;}


        @Override
        Set<String> propertyKeys() {
            return aggregations.keySet();
        }

        @Override
        Aggregation localAggregation(String propertyKey) {
            return aggregations.get(propertyKey);
        }

        @Override
        Optional<Aggregation> globalAggregation() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return aggregations.toString();
        }
    }
}
