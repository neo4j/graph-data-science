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
package org.neo4j.graphalgo.compat;

import org.neo4j.kernel.api.StatementConstants;

public final class StatementConstantsProxy {

    public static final int NO_SUCH_RELATIONSHIP_TYPE = StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
    public static final int NO_SUCH_LABEL = StatementConstants.NO_SUCH_LABEL;
    public static final int NO_SUCH_PROPERTY_KEY = StatementConstants.NO_SUCH_PROPERTY_KEY;
    public static final int ANY_LABEL = StatementConstants.ANY_LABEL;
    public static final int ANY_RELATIONSHIP_TYPE = StatementConstants.ANY_RELATIONSHIP_TYPE;

    private StatementConstantsProxy() {
        throw new UnsupportedOperationException();
    }
}
