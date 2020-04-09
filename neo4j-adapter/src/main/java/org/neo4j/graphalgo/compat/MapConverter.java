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
package org.neo4j.graphalgo.compat;

import org.neo4j.cypher.internal.evaluator.EvaluationException;
import org.neo4j.cypher.internal.evaluator.Evaluator;
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator;

import java.util.Map;

public final class MapConverter {

    private static final ExpressionEvaluator EVALUATOR = Evaluator.expressionEvaluator();

    @SuppressWarnings("unchecked")
    public static Map<String, Object> convert(String value) {
        try {
            return EVALUATOR.evaluate(value, Map.class);
        } catch (EvaluationException e) {
            throw new IllegalArgumentException(String.format("%s is not a valid map expression", value), e);
        }
    }

    private MapConverter() {
        throw new UnsupportedOperationException("No instances");
    }
}
