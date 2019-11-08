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
package org.neo4j.graphalgo;

import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TestLog implements Log {
    public static String DEBUG = "debug";
    public static String INFO = "info";
    public static String WARN = "warn";
    public static String ERROR = "error";


    private final Map<String, List<String>> messages;

    public TestLog() {
        messages = new HashMap<>(3);
    }

    public boolean containsMessage(String level, String fragment) {
        List<String> messageList = messages.getOrDefault(level, Collections.emptyList());
        return messageList.stream().anyMatch((message) -> message.contains(fragment));
    }

    @Override
    public void debug(String message) {
        logMessage(DEBUG, message);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        logMessage(DEBUG, message + " - Error: " + throwable.getMessage());
    }

    @Override
    public void debug(String format, Object... arguments) {
        logMessage(DEBUG, String.format(format, arguments));
    }

    @Override
    public void info(String message) {
        logMessage(INFO, message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        logMessage(INFO, message + " - Error: " + throwable.getMessage());
    }

    @Override
    public void info(String format, Object... arguments) {
        logMessage(INFO, String.format(format, arguments));
    }

    @Override
    public void warn(String message) {
        logMessage(WARN, message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        logMessage(WARN, message + " - Error: " + throwable.getMessage());
    }

    @Override
    public void warn(String format, Object... arguments) {
        logMessage(WARN, String.format(format, arguments));
    }

    @Override
    public void error(String message) {
        logMessage(ERROR, message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        logMessage(ERROR, message + " - Error: " + throwable.getMessage());
    }

    @Override
    public void error(String format, Object... arguments) {
        logMessage(ERROR, String.format(format, arguments));
    }

    @Override
    public void bulk(Consumer<Log> consumer) {
        consumer.accept(this);
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public Logger debugLogger() {
        return null;
    }

    @Override
    public Logger infoLogger() {
        return null;
    }

    @Override
    public Logger warnLogger() {
        return null;
    }

    @Override
    public Logger errorLogger() {
        return null;
    }

    private void logMessage(String level, String message) {
        List<String> messageList = messages.computeIfAbsent(level, (ignore) -> new ArrayList<>());
        messageList.add(message);
    }
}
