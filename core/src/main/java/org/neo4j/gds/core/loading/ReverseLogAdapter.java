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
package org.neo4j.gds.core.loading;

import org.neo4j.logging.Log;

/**
 * While we migrate, we will sometimes hit a leaf in our code that has not yet been migrated.
 * At that point we can reverse-adapt and wrap our log in something that fits Neo4j's Log interface.
 * Currently, we are wrapping Neo4j's log with our Log, so this is unwrap(wrap(log))...
 * It is temporary! We will get rid of Neo4j's log eventually.
 */
public class ReverseLogAdapter implements Log {
    private final org.neo4j.gds.logging.Log gdsLog;

    public ReverseLogAdapter(org.neo4j.gds.logging.Log gdsLog) {
        this.gdsLog = gdsLog;
    }

    @Override
    public boolean isDebugEnabled() {
        return gdsLog.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        throw new UnsupportedOperationException("TODOa");
    }

    @Override
    public void debug(String message, Throwable throwable) {
        throw new UnsupportedOperationException("TODOb");
    }

    @Override
    public void debug(String format, Object... arguments) {
        gdsLog.debug(format, arguments);
    }

    @Override
    public void info(String message) {
        gdsLog.info(message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        throw new UnsupportedOperationException("TODOd");
    }

    @Override
    public void info(String format, Object... arguments) {
        gdsLog.info(format, arguments);
    }

    @Override
    public void warn(String message) {
        throw new UnsupportedOperationException("TODOe");
    }

    @Override
    public void warn(String message, Throwable throwable) {
        throw new UnsupportedOperationException("TODOf");
    }

    @Override
    public void warn(String format, Object... arguments) {
        gdsLog.warn(format, arguments);
    }

    @Override
    public void error(String message) {
        throw new UnsupportedOperationException("TODOg");
    }

    @Override
    public void error(String message, Throwable throwable) {
        throw new UnsupportedOperationException("TODOh");
    }

    @Override
    public void error(String format, Object... arguments) {
        throw new UnsupportedOperationException("TODOi");
    }
}
