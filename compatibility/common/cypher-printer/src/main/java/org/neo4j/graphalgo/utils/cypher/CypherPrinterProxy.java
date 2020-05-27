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
package org.neo4j.graphalgo.utils.cypher;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;

import java.util.ServiceLoader;

public final class CypherPrinterProxy {

    private static final CypherPrinterApi IMPL;

    static {
        var neo4jVersion = GraphDatabaseApiProxy.neo4jVersion();
        CypherPrinterFactory kernelProxyFactory = ServiceLoader
            .load(CypherPrinterFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(f -> f.canLoad(neo4jVersion))
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load the " + CypherPrinterProxy.class + " implementation for " + neo4jVersion));
        IMPL = kernelProxyFactory.load();
    }

    /**
     * Renders any java type as a Cypher expression. Supported types are
     * primitives, CharSequences, Enums, Iterables, and Maps.
     * Empty lists and maps, as well as null, are considered to be "empty"
     * and will be ignored.
     *
     * @return A Cypher expression string for the type or the empty string if the type was empty
     * @throws IllegalArgumentException if the given type is not supported
     */
    public static @NotNull String toCypherString(@Nullable Object value) {
        return toCypherStringOr(value, "");
    }

    /**
     * Renders any java type as a Cypher expression. Supported types are
     * primitives, CharSequences, Enums, Iterables, and Maps.
     * Empty lists and maps, as well as null, are considered to be "empty"
     * and will be ignored.
     *
     * @return A Cypher expression string for the type or the given fallback value if the type was empty
     * @throws IllegalArgumentException if the given type is not supported
     */
    public static @NotNull String toCypherStringOr(
        @Nullable Object value,
        @NotNull String ifEmpty
    ) {
        return IMPL.toCypherStringOr(value, ifEmpty);
    }

    public static CypherPrinterApi.CypherParameter parameter(String value) {
        return CypherPrinterApi.param(value);
    }

    public static CypherPrinterApi.CypherVariable variable(String value) {
        return CypherPrinterApi.var(value);
    }

    private CypherPrinterProxy() {
    }
}
