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
package org.neo4j.gds.ml.pipeline;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NonEmptySetValidation {

    public static final int MIN_SET_SIZE = 1;
    public static final int MIN_TRAIN_SET_SIZE = 2; //At least 2 since this will be further split during cross validation
    public static final int MIN_TEST_COMPLEMENT_SET_SIZE = 3; // test-complement needs to be split in train and feature-input set


    private NonEmptySetValidation() {}

    public static void validateNodeSetSize(
        long numberNodesInSet,
        long minNumberNodes,
        String setName,
        String parameterName
    ) {
        validateElementSetIsNotEmpty(numberNodesInSet, minNumberNodes, setName, parameterName, "node(s)");
    }

    public static void validateRelSetSize(
        long numberNodesInSet,
        long minNumberNodes,
        String errorDesc,
        String parameterName
    ) {
        validateElementSetIsNotEmpty(numberNodesInSet, minNumberNodes, errorDesc, parameterName, "relationship(s)");
    }

    private static void validateElementSetIsNotEmpty(
        long elementsInSet,
        long expectedMinNumberOfElements,
        String errorDesc,
        String parameterName,
        String elementType
    ) {
        if (elementsInSet < expectedMinNumberOfElements) {
            throw new IllegalArgumentException(formatWithLocale(
                "The specified %s for the current graph. " +
                "The %s set would have %d %s " +
                "but it must have at least %d.",
                parameterName, errorDesc, elementsInSet, elementType, expectedMinNumberOfElements
            ));
        }
    }
}
