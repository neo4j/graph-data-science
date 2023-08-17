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
package org.neo4j.gds.extension;

import org.neo4j.gds.logging.Log;

/**
 * We wrap Neo4j's log so that we isolate our code from theirs.
 * This enables us to not have dependencies on Neo4j everywhere.
 * This class can live at the edge of our software and our domain code will be Neo4j (logging) agnostic.
 */
class LogAdapter implements Log {
    private final org.neo4j.logging.Log neo4jUserLog;

    LogAdapter(org.neo4j.logging.Log neo4jUserLog) {this.neo4jUserLog = neo4jUserLog;}

    @Override
    public void info(String message) {
        neo4jUserLog.info(message);
    }

    @Override
    public void info(String format, Object... arguments) {
        neo4jUserLog.info(format, arguments);
    }

    @Override
    public void warn(String message, Exception e) {
        neo4jUserLog.warn(message, e);
    }

    @Override
    public void warn(String format, Object... arguments) {
        neo4jUserLog.warn(format, arguments);
    }

    @Override
    public boolean isDebugEnabled() {
        return neo4jUserLog.isDebugEnabled();
    }

    @Override
    public void debug(String format, Object... arguments) {
        neo4jUserLog.debug(format, arguments);
    }

    @Override
    public Object getNeo4jLog() {
        return neo4jUserLog;
    }
}
