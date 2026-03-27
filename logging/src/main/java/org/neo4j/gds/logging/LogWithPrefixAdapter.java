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
package org.neo4j.gds.logging;

import java.util.Locale;

public class LogWithPrefixAdapter implements Log {
    private Log log;
    private final String prefix;

    public LogWithPrefixAdapter(Log log, String prefix) {
        this.log = log;
        this.prefix = prefix;
    }

    @Override
    public void info(String message) {
        log.info(prefix + message);
    }

    @Override
    public void info(String format, Object... arguments) {
        log.info(prefix + format, arguments);
    }

    @Override
    public void warn(String message, Exception e) {
        log.warn(prefix + message, e);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log.warn(prefix + format, arguments);
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(String format, Object... arguments) {
        log.debug(prefix + format, arguments);
    }

    @Override
    public void error(String errorMessage, Throwable throwable) {
        log.error(prefix + errorMessage, throwable);
    }

    @Override
    public void error(String messageFormat, Object... arguments) {
        log.error(prefix + messageFormat, arguments);
    }

    @Override
    public void error(String messageFormat, Throwable exception, Object... arguments) {
        log.error(String.format(Locale.US, prefix + messageFormat, arguments), exception);
    }
}
