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
package org.neo4j.gds.ml.decisiontree;

import org.neo4j.gds.annotation.Configuration;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface DecisionTreeTrainerConfig {

    @Configuration.IntegerRange(min = 1)
    default int maxDepth() {
        return Integer.MAX_VALUE;
    }

    @Configuration.IntegerRange(min = 2)
    default int minSplitSize() {
        return 2;
    }

    @Configuration.IntegerRange(min = 1)
    default int minLeafSize() {
        return 1;
    }

    @Configuration.Check
    default void validateMinSizes() {
        if (minLeafSize() >= minSplitSize()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Configuration parameter 'minLeafSize' which was equal to %d, must be strictly smaller than configuration parameter 'minSplitSize' which was equal to %d",
                minLeafSize(),
                minSplitSize()
            ));
        }
    }
}
