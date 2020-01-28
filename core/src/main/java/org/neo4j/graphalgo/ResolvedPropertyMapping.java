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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.Aggregation;

import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_PROPERTY_KEY;

@ValueClass
public abstract class ResolvedPropertyMapping {

    /**
     * property key in the result map Graph.nodeProperties(`propertyKey`)
     */
    public abstract String propertyKey();

    /**
     * property name in the graph (a:Node {`propertyKey`:xyz})
     */
    public abstract String neoPropertyKey();

    public abstract double defaultValue();

    public abstract Aggregation aggregation();

    /**
     * Property identifier from Neo4j token store
     */
    public abstract int propertyKeyId();

    public boolean exists() {
        return propertyKeyId() != NO_SUCH_PROPERTY_KEY;
    }
}
