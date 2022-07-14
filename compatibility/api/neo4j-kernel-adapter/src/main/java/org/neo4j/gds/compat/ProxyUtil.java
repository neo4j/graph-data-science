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

import org.neo4j.gds.annotation.SuppressForbidden;

import java.util.ServiceLoader;

public final class ProxyUtil {

    @SuppressForbidden(reason = "This is the best we can do at the moment")
    public static <PROXY, FACTORY extends ProxyFactory<PROXY>> PROXY findProxy(Class<FACTORY> factoryClass) {
        var log = new OutputStreamLogBuilder(System.out).build();
        log.info("Java vendor: %s", System.getProperty("java.vendor"));
        log.info("Java version: %s", System.getProperty("java.version"));
        log.info("Java home: %s", System.getProperty("java.home"));
        var neo4jVersion = GraphDatabaseApiProxy.neo4jVersion();
        log.info("Detected Neo4j version: %s", neo4jVersion);
        FACTORY proxyFactory = ServiceLoader
            .load(factoryClass)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(f -> {
                var canLoad = f.canLoad(neo4jVersion);
                log.info("GDS compatibility for %s: %s", f.description(), canLoad ? "available" : "not available");
                return canLoad;
            })
            .findFirst()
            .orElseThrow(() -> new LinkageError("GDS is not compatible with Neo4j version: " + neo4jVersion));
        var proxy = proxyFactory.load();
        log.info("Loaded compatibility proxy layer: %s", proxy);

        return proxy;
    }

    private ProxyUtil() {}
}
