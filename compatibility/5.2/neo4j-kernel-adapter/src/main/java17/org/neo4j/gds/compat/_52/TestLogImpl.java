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
package org.neo4j.gds.compat._52;

import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.TestLog;

import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class TestLogImpl implements TestLog {

    private final ConcurrentMap<String, ConcurrentLinkedQueue<String>> messages;

    TestLogImpl() {
        messages = new ConcurrentHashMap<>(3);
    }

    @Override
    public void assertContainsMessage(String level, String fragment) {
        if (!containsMessage(level, fragment)) {
            throw new RuntimeException(
                String.format(
                    Locale.US,
                    "Expected log output to contain `%s` for log level `%s`%nLog messages:%n%s",
                    fragment,
                    level,
                    String.join("\n", messages.get(level))
                )
            );
        }
    }

    @Override
    public boolean containsMessage(String level, String fragment) {
        ConcurrentLinkedQueue<String> messageList = messages.getOrDefault(level, new ConcurrentLinkedQueue<>());
        return messageList.stream().anyMatch((message) -> message.contains(fragment));
    }

    @Override
    public boolean hasMessages(String level) {
        return !messages.getOrDefault(level, new ConcurrentLinkedQueue<>()).isEmpty();
    }

    @Override
    public ArrayList<String> getMessages(String level) {
        return new ArrayList<>(messages.getOrDefault(level, new ConcurrentLinkedQueue<>()));
    }

    @SuppressForbidden(reason = "test log can print")
    public void printMessages() {
        System.out.println("TestLog Messages: " + messages);
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public void debug(String message) {
        logMessage(DEBUG, message);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        debug(String.format(Locale.US, "%s - %s", message, throwable.getMessage()));
    }

    @Override
    public void debug(String format, Object... arguments) {
        debug(String.format(Locale.US, format, arguments));
    }

    @Override
    public void info(String message) {
        logMessage(INFO, message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        info(String.format(Locale.US, "%s - %s", message, throwable.getMessage()));
    }

    @Override
    public void info(String format, Object... arguments) {
        info(String.format(Locale.US, format, arguments));
    }

    @Override
    public void warn(String message) {
        logMessage(WARN, message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        warn(String.format(Locale.US, "%s - %s", message, throwable.getMessage()));
    }

    @Override
    public void warn(String format, Object... arguments) {
        warn(String.format(Locale.US, format, arguments));
    }

    @Override
    public void error(String message) {
        logMessage(ERROR, message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        error(String.format(Locale.US, "%s - %s", message, throwable.getMessage()));
    }

    @Override
    public void error(String format, Object... arguments) {
        error(String.format(Locale.US, format, arguments));
    }

    private void logMessage(String level, String message) {
        messages.computeIfAbsent(
            level,
            (ignore) -> new ConcurrentLinkedQueue<>()
        ).add(message);
    }
}


