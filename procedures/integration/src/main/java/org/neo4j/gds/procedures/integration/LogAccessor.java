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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.logging.Log;
import org.neo4j.logging.internal.LogService;

/**
 * Just encapsulating this code which is used in two places
 */
public class LogAccessor {
    public Log getLog(LogService logService, Class<?> cls) {
        // We stack off the Neo4j's log and have our own
        var neo4jUserLog = Neo4jProxy.getUserLog(logService, cls);
        return new LogAdapter(neo4jUserLog);
    }
}
