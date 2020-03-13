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
package org.neo4j.graphalgo.functions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OneHotEncodingFuncTest {

    @Test
    void singleCategorySelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.singletonList("Italian");

        assertEquals(asList(1L, 0L, 0L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void noCategoriesSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.emptyList();

        assertEquals(asList(0L, 0L, 0L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void moreThanOneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese");

        assertEquals(asList(1L, 0L, 1L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void allSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese", "Indian");

        assertEquals(asList(1L, 1L, 1L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void nonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.singletonList("British");

        assertEquals(asList(0L, 0L, 0L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void oneNonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("British", "Chinese");

        assertEquals(asList(0L, 0L, 1L), new OneHotEncodingFunc().oneHotEncoding(values, selectedValues));
    }

    @Test
    void nullSelectedMeansNoneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");

        assertEquals(asList(0L, 0L, 0L), new OneHotEncodingFunc().oneHotEncoding(values, null));
    }

    @Test
    void nullAvailableMeansEmptyArray() {
        assertEquals(Collections.emptyList(), new OneHotEncodingFunc().oneHotEncoding(null, null));
    }

}