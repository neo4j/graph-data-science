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
package org.neo4j.graphalgo.doc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class CypherAsciidocTableConverter {

    private CypherAsciidocTableConverter() {}

    public static String asciidoc(String cypher) {
        List<String> lines = Arrays.stream(cypher.split(System.lineSeparator()))
            .filter(s -> !s.startsWith("+"))
            .map(l -> {
                int endIndex = l.lastIndexOf('|');
                return endIndex < 0 ? l : l.substring(0, endIndex).trim();
            }).collect(Collectors.toList());

        int columns = lines.get(0).split("\\|").length - 1;

        lines.remove(lines.size() - 1);
        lines.add(String.format("%d+|%d rows", columns, lines.size() - 1));
        lines.add("");

        return String.join(System.lineSeparator(), lines);
    }

    public static String cypher(String asciidoc) {
        Deque<String> lines = Arrays
            .stream(asciidoc.split(System.lineSeparator()))
            .collect(Collectors.toCollection(ArrayDeque::new));
        Optional<Integer> maxWidth = lines.stream().map(String::length).max(Integer::compare);
        int w = maxWidth.get();
        String separator = separator(w);
        String rowCountLine = lines.getLast();
        lines.removeLast();

        List<String> result = new ArrayList<>();
        result.add(separator);
        result.add(pad(lines.pop(), w));
        result.add(separator);
        while (!lines.isEmpty()) {
            result.add(pad(lines.pop(), w));
        }
        result.add(separator);
        result.add(rowCountLine.split("\\|")[1]);
        result.add("");
        return String.join(System.lineSeparator(), result);
    }

    public static String separator(int w) {
        StringBuilder sb = new StringBuilder().append("+");
        for (int i = 0; i < w; i++) {
            sb.append("-");
        }
        return sb.append("+").toString();
    }

    public static String pad(String s, int w) {
        StringBuilder sb = new StringBuilder().append(s);
        for (int i = 0; i < w - (s.length() - 1); i++) {
            sb.append(" ");
        }
        sb.append("|");
        return sb.toString();
    }

}
