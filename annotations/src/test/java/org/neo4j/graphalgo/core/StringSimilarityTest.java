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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.core.StringSimilarity.jaro;
import static org.neo4j.graphalgo.core.StringSimilarity.jaroWinkler;

class StringSimilarityTest {

    @Test
    void jaroBothEmpty() {
        assertEquals(1.0, jaro("", ""));
    }

    @Test
    void jaroFirstEmpty() {
        assertEquals(0.0, jaro("", "jaro"));
    }

    @Test
    void jaroSecondEmpty() {
        assertEquals(0.0, jaro("distance", ""));
    }

    @Test
    void jaroSame() {
        assertEquals(1.0, jaro("jaro", "jaro"));
    }

    @Test
    void jaroMultibyte() {
        assertEquals(0.818, jaro("testabctest", "testöঙ香test"), 0.001);
        assertEquals(0.818, jaro("testöঙ香test", "testabctest"), 0.001);
    }

    @Test
    void jaroDiffShort() {
        assertEquals(0.767, jaro("dixon", "dicksonx"), 0.001);
    }

    @Test
    void jaroDiffOneCharacter() {
        assertEquals(0.0, jaro("a", "b"));
    }

    @Test
    void jaroSameOneCharacter() {
        assertEquals(1.0, jaro("a", "a"));
    }

    @Test
    void jaroDiffOneAndTwo() {
        assertEquals(0.83, jaro("a", "ab"), 0.01);
    }

    @Test
    void jaroDiffTwoAndOne() {
        assertEquals(0.83, jaro("ab", "a"), 0.01);
    }

    @Test
    void jaroDiffNoTransposition() {
        assertEquals(0.822, jaro("dwayne", "duane"), 0.001);
    }

    @Test
    void jaroDiffWithTransposition() {
        assertEquals(0.944, jaro("martha", "marhta"), 0.001);
    }

    @Test
    void jaroNames() {
        assertEquals(0.392, jaro(
            "Friedrich Nietzsche",
            "Jean-Paul Sartre"
        ), 0.001);
    }

    @Test
    void jaroWinklerBothEmpty() {
        assertEquals(1.0, jaroWinkler("", ""));
    }

    @Test
    void jaroWinklerFirstEmpty() {
        assertEquals(0.0, jaroWinkler("", "jaro-winkler"));
    }

    @Test
    void jaroWinklerSecondEmpty() {
        assertEquals(0.0, jaroWinkler("distance", ""));
    }

    @Test
    void jaroWinklerSame() {
        assertEquals(1.0, jaroWinkler("Jaro-Winkler", "Jaro-Winkler"));
    }

    @Test
    void jaroWinklerMultibyte() {
        assertEquals(0.891, jaroWinkler("testabctest", "testöঙ香test"), 0.001);
        assertEquals(0.891, jaroWinkler("testöঙ香test", "testabctest"), 0.001);
    }

    @Test
    void jaroWinklerDiffShort() {
        assertEquals(0.813, jaroWinkler("dixon", "dicksonx"), 0.001);
        assertEquals(0.813, jaroWinkler("dicksonx", "dixon"), 0.001);
    }

    @Test
    void jaroWinklerDiffOneCharacter() {
        assertEquals(0.0, jaroWinkler("a", "b"));
    }

    @Test
    void jaroWinklerSameOneCharacter() {
        assertEquals(1.0, jaroWinkler("a", "a"));
    }

    @Test
    void jaroWinklerDiffNoTransposition() {
        assertEquals(0.840, jaroWinkler("dwayne", "duane"), 0.001);
    }

    @Test
    void jaroWinklerDiffWithTransposition() {
        assertEquals(0.961, jaroWinkler("martha", "marhta"), 0.001);
    }

    @Test
    void jaroWinklerNames() {
        assertEquals(0.562, jaroWinkler(
            "Friedrich Nietzsche",
            "Fran-Paul Sartre"
        ), 0.001);
    }

    @Test
    void jaroWinklerLongPrefix() {
        assertEquals(0.889, jaroWinkler("cheeseburger", "cheese fries"), 0.001);
    }

    @Test
    void jaroWinklerMoreNames() {
        assertEquals(0.868, jaroWinkler("Thorkel", "Thorgier"), 0.001);
    }

    @Test
    void jaroWinklerLengthOfOne() {
        assertEquals(0.738, jaroWinkler("Dinsdale", "D"), 0.001);
    }

    @Test
    void jaroWinklerVeryLongPrefix() {
        assertEquals(0.988, jaroWinkler(
            "thequickbrownfoxjumpedoverx",
            "thequickbrownfoxjumpedovery"
        ), 0.001);
    }

}
