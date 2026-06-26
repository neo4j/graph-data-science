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
package org.neo4j.gds.assertj;

import org.assertj.core.presentation.StandardRepresentation;
import org.neo4j.gds.mem.MemoryRange;

import static org.neo4j.gds.utils.StringFormatting.formatNumber;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryRangeRepresentation extends StandardRepresentation {

    @Override
    public String fallbackToStringOf(Object o) {
        if (o instanceof MemoryRange memoryRange) {
            if (memoryRange.min == memoryRange.max) {
                return formatNumber(memoryRange.min);
            } else {
                return formatWithLocale("(%s, %s)", formatNumber(memoryRange.min), formatNumber(memoryRange.max));
            }
        }
        // fallback to default formatting.
        return super.fallbackToStringOf(o);
    }
}
