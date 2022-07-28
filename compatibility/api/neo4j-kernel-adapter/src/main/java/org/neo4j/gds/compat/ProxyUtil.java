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
import org.neo4j.logging.Log;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ProxyUtil {

    private static final AtomicBoolean LOG_ENVIRONMENT = new AtomicBoolean(true);

    @SuppressForbidden(reason = "This is the best we can do at the moment")
    public static <PROXY, FACTORY extends ProxyFactory<PROXY>> PROXY findProxy(Class<FACTORY> factoryClass) {
        var log = new OutputStreamLogBuilder(System.out).build();
        var neo4jVersion = new AtomicReference<Neo4jVersion>();
        var availabilityLog = new StringJoiner(", ", "GDS compatibility: ", "");
        try {
            neo4jVersion.set(GraphDatabaseApiProxy.neo4jVersion());
            FACTORY proxyFactory = ServiceLoader
                .load(factoryClass)
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(f -> {
                    var canLoad = f.canLoad(neo4jVersion.get());
                    availabilityLog.add(String.format(
                        Locale.ENGLISH,
                        "for %s -- %s",
                        f.description(),
                        canLoad ? "available" : "not available"
                    ));
                    return canLoad;
                })
                .findFirst()
                .orElseThrow(() -> new LinkageError(String.format(
                    Locale.ENGLISH,
                    "GDS %s is not compatible with Neo4j version: %s",
                    gdsVersion(log),
                    neo4jVersion
                )));
            availabilityLog.add("selected: " + proxyFactory.description());
            return proxyFactory.load();
        } finally {
            if (LOG_ENVIRONMENT.getAndSet(false)) {
                log.debug(
                    "Java vendor: [%s] Java version: [%s] Java home: [%s] GDS version: [%s] Detected Neo4j version: [%s]",
                    System.getProperty("java.vendor"),
                    System.getProperty("java.version"),
                    System.getProperty("java.home"),
                    gdsVersion(log),
                    neo4jVersion
                );
            }
            log.info(availabilityLog.toString());
        }
    }

    private static final AtomicReference<String> GDS_VERSION = new AtomicReference<>();

    private static String gdsVersion(Log log) {
        if (GDS_VERSION.get() == null) {
            GDS_VERSION.set(getGdsVersion(log));
        }

        return GDS_VERSION.get();
    }

    private static String getGdsVersion(Log log) {
        try {
            // The class that we use to get the GDS version lives in proc-sysinfo, which is part of the released GDS jar,
            // but we don't want to depend on that here. One reason is that this class gets generated and re-generated
            // on every build and having it at the top of the dependency graph would cause a lot of recompilation.
            // Let's do a bit of class loading and reflection to get the version.
            var lookup = MethodHandles.lookup();

            var buildInfoPropertiesClass = Class.forName("org.neo4j.gds.BuildInfoProperties");

            // equivalent to: BuildInfoProperties.get()
            var buildInfoPropertiesHandle = lookup.findStatic(
                buildInfoPropertiesClass,
                "get",
                MethodType.methodType(buildInfoPropertiesClass)
            );

            // equivalent to: buildInfoProperties.gdsVersion()
            var gdsVersionHandle = lookup.findVirtual(
                buildInfoPropertiesClass,
                "gdsVersion",
                MethodType.methodType(String.class)
            );

            // var buildInfoProperties = BuildInfoProperties.get()
            var buildInfoProperties = buildInfoPropertiesHandle.invoke();
            // var gdsVersion = buildInfoProperties.gdsVersion()
            var gdsVersion = gdsVersionHandle.invoke(buildInfoProperties);
            return String.valueOf(gdsVersion);
        } catch (ClassNotFoundException e) {
            log.warn("Could not determine GDS version, BuildInfoProperties is missing.", e);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            log.warn("Could not determine GDS version, the according methods on BuildInfoProperties could not be found.", e);
        } catch (Throwable e) {
            log.warn("Could not determine GDS version, the according methods on BuildInfoProperties failed.", e);
        }

        return "Unknown";
    }

    private ProxyUtil() {}
}
