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
package org.neo4j.gds.compat;

import org.intellij.lang.annotations.Language;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.logging.Log;

import java.util.ArrayList;

public interface TestLog extends Log {
    String DEBUG = "debug";
    String INFO = "info";
    String WARN = "warn";
    String ERROR = "error";

    void assertContainsMessage(String level, String fragment);

    void assertMessageMatches(String level, @Language("RegExp") String pattern);

    boolean containsMessage(String level, String fragment);

    boolean hasMessages(String level);

    ArrayList<String> getMessages(String level);

    @SuppressForbidden(reason = "test log can print")
    void printMessages();

    boolean isDebugEnabled();
}


