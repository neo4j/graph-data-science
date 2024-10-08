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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class ProxyUtil {

    private static final AtomicBoolean LOG_ENVIRONMENT = new AtomicBoolean(true);
    private static final AtomicReference<ProxyLog> LOG_MESSAGES = new AtomicReference<>(new BufferingLog());

    private static final Map<Class<?>, ProxyInfo<?, ?>> PROXY_INFO_CACHE = new ConcurrentHashMap<>();

    public static <PROXY, FACTORY extends ProxyFactory<PROXY>> PROXY findProxy(
        Class<FACTORY> factoryClass,
        MayLogToStdout mayLogToStdout
    ) {
        return findProxyInfo(factoryClass)
            .proxy()
            .apply(mayLogToStdout);
    }

    public static <PROXY, FACTORY extends ProxyFactory<PROXY>> ProxyInfo<FACTORY, PROXY> findProxyInfo(Class<FACTORY> factoryClass) {
        // we know that this type is correct due to the signature of loadAndValidateProxyInfo
        // we lose the type information because of the map API
        //noinspection unchecked
        return (ProxyInfo<FACTORY, PROXY>) PROXY_INFO_CACHE.computeIfAbsent(
            factoryClass,
            fc -> loadAndValidateProxyInfo(factoryClass)
        );
    }

    public static void dumpLogMessages(Log log) {
        var buffer = LOG_MESSAGES.getAndSet(NullLog.INSTANCE);
        buffer.replayInto(log);
    }

    private static <PROXY, FACTORY extends ProxyFactory<PROXY>> ProxyInfo<FACTORY, PROXY> loadAndValidateProxyInfo(Class<FACTORY> factoryClass) {
        var log = LOG_MESSAGES.get();
        var availabilityLog = new StringJoiner(", ", "GDS compatibility: ", "");
        availabilityLog.setEmptyValue("");

        var proxyInfo = loadProxyInfo(factoryClass);

        // log any errors while looking for the GDS version, but continue since we have a fallback value
        proxyInfo.gdsVersion().error().ifPresent(e -> e.log(log));

        try {

            // log any errors while looking for the Neo4j version
            // stop execution since we don't have a valid Neo4j version fallback value
            var neo4jVersionError = proxyInfo.neo4jVersion().error();
            if (neo4jVersionError.isPresent()) {
                neo4jVersionError.get().log(log);
                throw new RuntimeException(neo4jVersionError.get().reason());
            }

            // log any errors while trying to find all proxies
            // stop execution since we don't have any proxy instances
            var proxyError = proxyInfo.error();
            if (proxyError.isPresent()) {
                proxyError.get().log(log);
                throw new RuntimeException(proxyError.get().reason());
            }

            // log availability of all proxy implementations
            proxyInfo.availability().forEach((name, availability) -> {
                availabilityLog.add(String.format(
                    Locale.ENGLISH,
                    "for %s -- %s",
                    name,
                    availability ? "available" : "not available"
                ));
            });

            // make sure that we have a proxy available

            var factory = proxyInfo.factory();
            if (factory.isEmpty()) {
                return proxyInfo;
            }

            availabilityLog.add("selected: " + factory.get().description());

            var builder = ProxyInfoBuilder.builder(proxyInfo);
            var proxy = factory.get().load();
            builder.maybeProxy(__ -> proxy);
            return builder.build();

        } finally {
            if (LOG_ENVIRONMENT.getAndSet(false)) {
                log.log(
                    LogLevel.DEBUG,
                    "Java vendor: [%s] Java version: [%s] Java home: [%s] GDS version: [%s] Detected Neo4j version: [%s]",
                    proxyInfo.javaInfo().javaVendor(),
                    proxyInfo.javaInfo().javaVersion(),
                    proxyInfo.javaInfo().javaHome(),
                    proxyInfo.gdsVersion().gdsVersion(),
                    proxyInfo.neo4jVersion().neo4jVersion()
                );
            }
            var availability = availabilityLog.toString();
            if (!availability.isEmpty()) {
                log.log(LogLevel.INFO, availability);
            }
        }
    }

    private static <PROXY, FACTORY extends ProxyFactory<PROXY>> ProxyInfo<FACTORY, PROXY> loadProxyInfo(Class<FACTORY> factoryClass) {
        var builder = ProxyInfoBuilder
            .<FACTORY, PROXY>builder()
            .availability(new LinkedHashMap<>())
            .factoryType(factoryClass)
            .neo4jVersion(NEO4J_VERSION_INFO)
            .gdsVersion(GdsVersionInfoProvider.GDS_VERSION_INFO)
            .javaInfo(JAVA_INFO);

        try {
            var availableProxies = ServiceLoader
                .load(factoryClass, factoryClass.getClassLoader())
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(f -> {
                    var canLoad = f.canLoad(NEO4J_VERSION_INFO.neo4jVersion());
                    builder.availability().put(f.description(), canLoad);
                    return canLoad;
                })
                .toList();

            builder.factory(availableProxies.stream().findFirst());
        } catch (Exception e) {
            builder.error(new ErrorInfo("Could not load GDS proxy: " + e.getMessage(), LogLevel.ERROR, e));
        }

        return builder.build();
    }

    private static final Neo4jVersionInfo NEO4J_VERSION_INFO = loadNeo4jVersion();

    private static Neo4jVersionInfo loadNeo4jVersion() {
        try {
            var neo4jVersion = Neo4jVersion.findNeo4jVersion();
            var error =
                neo4jVersion.isSupported()
                    ? Optional.<ErrorInfo>empty()
                    : Optional.of(new ErrorInfo(
                        "GDS does not support Neo4j version " + neo4jVersion,
                        LogLevel.ERROR,
                        new UnsupportedOperationException("GDS does not support Neo4j version " + neo4jVersion)
                    ));

            return new Neo4jVersionInfo(neo4jVersion, error);
        } catch (Exception e) {
            var error = new ErrorInfo("Could not determine Neo4j version: " + e.getMessage(), LogLevel.ERROR, e);
            return new Neo4jVersionInfo(new Neo4jVersion.Unsupported(-1, -1, "unknown"), Optional.of(error));
        }
    }

    private static final JavaInfo JAVA_INFO = loadJavaInfo();

    private static JavaInfo loadJavaInfo() {
        return new JavaInfo(
            System.getProperty("java.vendor"),
            System.getProperty("java.version"),
            System.getProperty("java.home")
        );
    }

    /**
     * We want to test this class as if we just started the JVM,
     * i.e. the state of whether we have already logged is not shared.
     * <p>
     * This is not tied to the lifecycle of the test instance but to the
     * lifecycle of the JVM.
     * <p>
     * We allow tests to reset the ProxyUtil with a test-only method to
     * avoid having to fork a new JVM fer every test.
     */
    @TestOnly
    static void resetState() {
        LOG_ENVIRONMENT.set(true);
        LOG_MESSAGES.set(new BufferingLog());
        PROXY_INFO_CACHE.clear();
    }

    public enum MayLogToStdout {
        YES, NO
    }

    @GenerateBuilder
    public record ProxyInfo<T, U>(
        @NotNull Class<T> factoryType,
        @NotNull Neo4jVersionInfo neo4jVersion,
        @NotNull GdsVersionInfoProvider.GdsVersionInfo gdsVersion,
        @NotNull JavaInfo javaInfo,
        @NotNull LinkedHashMap<String, Boolean> availability,
        @NotNull Optional<T> factory,
        @NotNull Optional<ErrorInfo> error,
        @NotNull Optional<Function<MayLogToStdout, U>> maybeProxy
    ) {
        Function<MayLogToStdout, U> proxy() {
            return this.maybeProxy.orElse(this::unsupported);
        }

        @SuppressForbidden(reason = "We need to log to stdout here")
        private U unsupported(MayLogToStdout mayLogToStdout) {
            if (mayLogToStdout == MayLogToStdout.YES) {
                // since we are throwing and potentially aborting the database startup, we might as well
                // log all messages we have accumulated so far to provide more debugging context
                ProxyUtil.dumpLogMessages(OutputStreamLog.builder(System.out).build().log());
            }

            throw new LinkageError(String.format(
                Locale.ENGLISH,
                "GDS %s is not compatible with Neo4j version: %s",
                gdsVersion.gdsVersion(),
                neo4jVersion.neo4jVersion().fullVersion()
            ));
        }
    }

    public record Neo4jVersionInfo(
        @NotNull Neo4jVersion neo4jVersion,
        @NotNull Optional<ErrorInfo> error
    ) {
    }

    public record ErrorInfo(
        @NotNull String message,
        @NotNull LogLevel logLevel,
        @NotNull Throwable reason
    ) {
        void log(ProxyLog log) {
            log.log(logLevel(), message(), reason());
        }
    }

    public enum LogLevel {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    public record JavaInfo(
        @NotNull String javaVendor,
        @NotNull String javaVersion,
        @NotNull String javaHome
    ) {
    }

    interface ProxyLog {
        void log(LogLevel logLevel, String message, Throwable reason);

        void log(LogLevel logLevel, String format, Object... args);

        void replayInto(Log log);
    }

    private record LogMessage(
        @NotNull LogLevel logLevel,
        @NotNull String message,
        Optional<Throwable> reason,
        Optional<Object[]> args
    ) {
    }

    private static final class BufferingLog implements ProxyLog {
        private final Collection<LogMessage> messages = new ArrayList<>();

        @Override
        public void log(LogLevel logLevel, String message, Throwable reason) {
            messages.add(new LogMessage(logLevel, message, Optional.ofNullable(reason), Optional.empty()));
        }

        @Override
        public void log(LogLevel logLevel, String format, Object... args) {
            messages.add(new LogMessage(logLevel, format, Optional.empty(), Optional.of(args)));
        }

        @Override
        public void replayInto(Log log) {
            messages.forEach(message -> {
                message.reason().ifPresent(reason -> {
                    switch (message.logLevel()) {
                        case DEBUG -> log.debug(message.message(), reason);
                        case INFO -> log.info(message.message(), reason);
                        case WARN -> log.warn(message.message(), reason);
                        case ERROR -> log.error(message.message(), reason);
                    }
                });
                message.args().ifPresent(args -> {
                    switch (message.logLevel()) {
                        case DEBUG -> log.debug(message.message(), args);
                        case INFO -> log.info(message.message(), args);
                        case WARN -> log.warn(message.message(), args);
                        case ERROR -> log.error(message.message(), args);
                    }
                });
            });
            // drop messages from memory
            messages.clear();
        }
    }

    private enum NullLog implements ProxyLog {
        INSTANCE;

        @Override
        public void log(LogLevel logLevel, String message, Throwable reason) {
            // do nothing
        }

        @Override
        public void log(LogLevel logLevel, String format, Object... args) {
            // do nothing
        }

        @Override
        public void replayInto(Log log) {
            // do nothing
        }
    }

    private ProxyUtil() {}
}
