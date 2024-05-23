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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

/**
 * I wish this did not exist quite like this; it is where we encapsulate running an algorithm,
 * managing termination, and handling (progress tracker) resources.
 * Somehow I wish that was encapsulated more naturally, but as you can hear from this use of language,
 * the design has not crystallized yet.
 * At least nothing here is tied to termination flag.
 */
public class AlgorithmMachinery {
    /**
     * Runs algorithm.
     * Optionally releases progress tracker.
     * Exceptionally marks progress tracker state as failed.
     *
     * @return algorithm result, or an error in the form of an exception
     */
    public <RESULT> RESULT runAlgorithmsAndManageProgressTracker(
        Algorithm<RESULT> algorithm,
        ProgressTracker progressTracker,
        boolean shouldReleaseProgressTracker
    ) {
        try {
            return algorithm.compute();
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        } finally {
            if (shouldReleaseProgressTracker) progressTracker.release();
        }
    }
}
