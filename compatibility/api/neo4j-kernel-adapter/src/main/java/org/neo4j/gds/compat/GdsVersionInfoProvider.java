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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.annotation.SuppressForbidden;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class GdsVersionInfoProvider {

    private GdsVersionInfoProvider() {}

    public static final GdsVersionInfo GDS_VERSION_INFO = loadGdsVersion();

    @GenerateBuilder
    public record GdsVersionInfo(String rawGdsVersion, AtomicReference<Optional<ErrorInfo>> error) {
        @SuppressForbidden(reason = "We can't do any better with this")
        public String gdsVersion() {
            this.error
                .getAndSet(Optional.empty())
                .ifPresent(err -> err.log(System.out));
            return this.rawGdsVersion;
        }
    }

    private static GdsVersionInfo loadGdsVersion() {
        var builder = GdsVersionInfoBuilder.builder()
            .rawGdsVersion("unknown")
            .error(new AtomicReference<>(Optional.empty()));
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

            builder.rawGdsVersion(String.valueOf(gdsVersion));
        } catch (ClassNotFoundException e) {
            builder.error().set(Optional.of(new ErrorInfo(
                "Could not determine GDS version, BuildInfoProperties is missing. " +
                    "This is likely due to not running GDS as a plugin, " +
                    "for example when running tests or using GDS as a Java module dependency.",
                LogLevel.INFO,
                e
            )));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            builder.error().set(Optional.of(new ErrorInfo(
                "Could not determine GDS version, the according methods on BuildInfoProperties could not be found.",
                LogLevel.WARN,
                e
            )));
        } catch (Throwable e) {
            builder.error().set(Optional.of(new ErrorInfo(
                "Could not determine GDS version, the according methods on BuildInfoProperties failed.",
                LogLevel.WARN,
                e
            )));
        }

        return builder.build();
    }

    record ErrorInfo(
        @NotNull String message,
        @NotNull LogLevel logLevel,
        @NotNull Throwable reason
    ) {
        void log(PrintStream out) {
            switch (logLevel) {
                case INFO -> out.printf(Locale.ENGLISH, "[info] %s: %s%n", this.message, this.reason.getMessage());
                case WARN -> out.printf(Locale.ENGLISH, "[warn] %s: %s%n", this.message, this.reason.getMessage());
            };
            this.reason.printStackTrace(out);
        }
    }

    enum LogLevel {
        INFO,
        WARN,
    }
}
