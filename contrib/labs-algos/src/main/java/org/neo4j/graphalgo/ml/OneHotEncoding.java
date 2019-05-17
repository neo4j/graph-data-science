/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.ml;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class OneHotEncoding {

    @UserFunction("algo.ml.oneHotEncoding")
    @Description("CALL algo.ml.oneHotEncoding(availableValues, selectedValues) - return a list of selected values in a one hot encoding format.")
    public List<Long> oneHotEncoding(@Name(value = "availableValues") List<Object> availableValues,
                                     @Name(value = "selectedValues") List<Object> selectedValues) {
        if (availableValues == null) {
            return LongStream.empty().boxed().collect(Collectors.toList());
        }

        if (selectedValues == null) {
            return LongStream.range(0, availableValues.size()).map(index -> 0).boxed().collect(Collectors.toList());
        }

        Set<Object> selectedValuesSet = new HashSet<>(selectedValues);
        Object[] availableValuesArray = availableValues.toArray();
        return LongStream.range(0, availableValues.size())
                .map(index -> selectedValuesSet.contains(availableValuesArray[(int) index]) ? 1L : 0L)
                .boxed()
                .collect(Collectors.toList());
    }
}
