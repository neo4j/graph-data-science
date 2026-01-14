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

import org.neo4j.gds.numbers.Aggregation;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class ToUndirectedAggregations {

    public abstract Set<String> propertyKeys();

    public abstract Aggregation localAggregation(String propertyKey);

    public abstract Optional<Aggregation> globalAggregation();


    public static String toString(ToUndirectedAggregations aggregations) {
        return aggregations.toString();
    }

    public static final class GlobalAggregation extends ToUndirectedAggregations {
       private final Aggregation aggregation;

        public GlobalAggregation(Aggregation aggregation) {this.aggregation = aggregation;}

        @Override
        public Set<String> propertyKeys() {
            return Set.of();
        }

        @Override
        public Aggregation localAggregation(String propertyKey) {
            return aggregation;
        }

        @Override
        public Optional<Aggregation> globalAggregation() {
            return Optional.of(aggregation);
        }

        @Override
        public String toString() {
            return aggregation.toString();
        }
    }


    public static final class AggregationPerProperty extends ToUndirectedAggregations {
        private final Map<String, Aggregation> aggregations;

        public AggregationPerProperty(Map<String, Aggregation> aggregations) {this.aggregations = aggregations;}


        @Override
        public Set<String> propertyKeys() {
            return aggregations.keySet();
        }

        @Override
        public Aggregation localAggregation(String propertyKey) {
            return aggregations.get(propertyKey);
        }

        @Override
        public Optional<Aggregation> globalAggregation() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return aggregations.toString();
        }
    }
}
