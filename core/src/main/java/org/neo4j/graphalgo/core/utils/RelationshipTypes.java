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

package org.neo4j.graphalgo.core.utils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class RelationshipTypes {

    private static final char BACKTICK = '`';
    private static final Pattern PIPE = Pattern.compile("\\|");
    private static final Pattern IGNORE_CHARS = Pattern.compile("[<>:]");

    public static Set<String> parse(CharSequence relTypes) {
        if (relTypes == null) {
            return Collections.emptySet();
        }
        return PIPE.splitAsStream(relTypes)
                .map(RelationshipTypes::parseSingle)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static String parseSingle(String def) {
        return relationshipTypeFor(def);
    }

    private static String relationshipTypeFor(String name) {
        if (name.indexOf(BACKTICK) >= 0) {
            name = name.substring(name.indexOf(BACKTICK) + 1, name.lastIndexOf(BACKTICK));
        } else {
            name = IGNORE_CHARS.matcher(name).replaceAll("");
        }
        return name.trim();
    }


    private RelationshipTypes() {
        throw new UnsupportedOperationException("No instances");
    }
}
