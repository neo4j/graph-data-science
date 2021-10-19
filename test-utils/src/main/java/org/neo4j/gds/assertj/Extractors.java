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

import org.assertj.core.api.iterable.ThrowingExtractor;
import org.jetbrains.annotations.NotNull;

public final class Extractors {

    private Extractors() {}

    @NotNull
    public static ThrowingExtractor<String, String, RuntimeException> removingThreadId() {
        return message -> message
            .substring(message.indexOf("] ") + 2);
    }

    @NotNull
    public static ThrowingExtractor<String, String, RuntimeException> replaceTimings() {
        return message -> message.replaceAll("(\\d+\\s*)(ms|s|min)", "`some time`");
    }
}
