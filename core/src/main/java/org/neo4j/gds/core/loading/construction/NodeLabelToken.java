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
package org.neo4j.gds.core.loading.construction;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface NodeLabelToken {

    /**
     * @return `true` iff no label information was provided at all.
     */
    boolean isMissing();

    /**
     * @return `true` iff the provided label information could not be mapped to an internal type
     *         because it was provided as a wrong type.
     */
    boolean isInvalid();

    /**
     * @return `true` if the provided label information does not actually contain any labels or
     *         if no label information was provided at all.
     */
    boolean isEmpty();

    int size();

    @NotNull NodeLabel get(int index);

    String[] getStrings();

    /**
     * @return a stream of {@link org.neo4j.gds.NodeLabel}s represented by this token.
     */
    default Stream<NodeLabel> nodeLabels() {
        return IntStream.range(0, this.size()).mapToObj(this::get);
    }
}


