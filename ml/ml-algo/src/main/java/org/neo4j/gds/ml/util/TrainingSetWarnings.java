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
package org.neo4j.gds.ml.util;

import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TrainingSetWarnings {

    private static final int RECOMMENDED_MIN_ELEMENTS_PER_SET = 5;

    private TrainingSetWarnings() {}

    public static void warnForSmallNodeSets(long trainSetSize, long testSetSize, long validationFolds, ProgressTracker progressTracker) {
        warnForSmallSets(trainSetSize, testSetSize, validationFolds, "node", progressTracker);
    }

    public static void warnForSmallRelationshipSets(long trainSetSize, long testSetSize, long validationFolds, ProgressTracker progressTracker) {
        warnForSmallSets(trainSetSize, testSetSize, validationFolds, "relationship", progressTracker);
    }

    private static void warnForSmallSets(long trainSetSize, long testSetSize, long validationFolds, String elementType, ProgressTracker progressTracker) {
        progressTracker.logInfo("Train set size is " + trainSetSize);
        progressTracker.logInfo("Test set size is " + testSetSize);

        if (testSetSize < RECOMMENDED_MIN_ELEMENTS_PER_SET) {
            progressTracker.logWarning(formatWithLocale(
                "The specified `testFraction` leads to a very small test set " +
                "with only %d %s(s). Proceeding with such a small set might lead to unreliable results.",
                testSetSize, elementType
            ));
        }

        long validationSetSize = trainSetSize / validationFolds;
        //No need to check train set as it is always larger or equal to validation set.
        if (validationSetSize < RECOMMENDED_MIN_ELEMENTS_PER_SET) {
            progressTracker.logWarning(formatWithLocale(
                "The specified `validationFolds` leads to very small validation sets " +
                "with only %d %s(s). Proceeding with such small sets might lead to unreliable results.",
                validationSetSize, elementType
            ));
        }
    }
}
