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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.List;
import java.util.Locale;

import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

public interface Aggregator {
    Matrix aggregate(Matrix previousLayerRepresentations, int[][] adjacencyMatrix, int[] selfAdjacencyMatrix);

    List<Weights> weights();

    enum AggregatorType {
        MEAN,
        POOL;

        public static AggregatorType of(String activationFunction) {
            return valueOf(toUpperCaseWithLocale(activationFunction));
        }

        public static AggregatorType parse(Object object) {
            if (object == null) {
                return null;
            }
            if (object instanceof String) {
                return of(((String) object).toUpperCase(Locale.ENGLISH));
            }
            if (object instanceof AggregatorType) {
                return (AggregatorType) object;
            }
            return null;
        }

        public static String toString(AggregatorType af) {
            return af.toString();
        }
    }
}
