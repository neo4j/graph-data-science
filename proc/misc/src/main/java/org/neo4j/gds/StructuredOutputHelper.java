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
package org.neo4j.gds;

import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class StructuredOutputHelper {

    static final String UNKNOWN = "n/a";

    static final String TASK_BRANCH_TOKEN = "|-- ";
    static final String TASK_LEVEL_INDENTATION = "    ";

    private StructuredOutputHelper() {}

    /**
     * Produce a progress bar string in the format of: [######~~~~]
     */
    static String progressBar(long progress, long volume, int progressBarLength) {
        if (volume == Task.UNKNOWN_VOLUME) {
            return formatWithLocale("[~~~~%s~~~]", UNKNOWN);
        }

        var progressPercentage = relativeProgress(progress, volume);
        var scaledPercentage = (int) (progressPercentage * progressBarLength);

        var filledProgressBar = "#".repeat(scaledPercentage);
        var remainingProgressBar = "~".repeat(progressBarLength - scaledPercentage);

        var progressBarContent = filledProgressBar + remainingProgressBar;

        return formatWithLocale("[%s]", progressBarContent);
    }

    private static double relativeProgress(long progress, long volume) {
        return volume == 0
            ? 1.0D
            : ((double) progress) / volume;
    }

    public static String computeProgress(long progress, long volume) {
        if (volume == Task.UNKNOWN_VOLUME) {
            return UNKNOWN;
        }

        var progressPercentage = relativeProgress(progress, volume);
        var decimalFormat = new DecimalFormat("###.##%", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        return decimalFormat.format(progressPercentage);
    }

    static String treeViewDescription(String description, int depth) {
        return TASK_LEVEL_INDENTATION.repeat(depth) +
               TASK_BRANCH_TOKEN +
               description;
    }
}
