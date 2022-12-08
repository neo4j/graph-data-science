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
package positive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class ToMapConfig implements ToMap {
    private int foo;

    private long bar;

    private double baz;

    private Optional<Long> maybeBar;

    private Optional<Double> maybeBaz;

    public ToMapConfig(int foo, @NotNull CypherMapAccess config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.foo = foo;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.bar = config.requireLong("bar");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.baz = config.requireDouble("baz");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.maybeBar = CypherMapAccess.failOnNull("maybeBar", config.getOptional("maybeBar", Long.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.maybeBaz = CypherMapAccess.failOnNull("maybeBaz", config.getOptional("maybeBaz", Double.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            if (errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors
                    .stream()
                    .map(IllegalArgumentException::getMessage)
                    .collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t",
                        "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t",
                        ""
                    ));
                IllegalArgumentException combinedError = new IllegalArgumentException(combinedErrorMsg);
                errors.forEach(error -> combinedError.addSuppressed(error));
                throw combinedError;
            }
        }
    }

    @Override
    public int foo() {
        return this.foo;
    }

    @Override
    public long bar() {
        return this.bar;
    }

    @Override
    public double baz() {
        return this.baz;
    }

    @Override
    public Optional<Long> maybeBar() {
        return this.maybeBar;
    }

    @Override
    public Optional<Double> maybeBaz() {
        return this.maybeBaz;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("bar", bar());
        map.put("baz", positive.ToMap.add42(baz()));
        maybeBar().ifPresent(maybeBar -> map.put("maybeBar", maybeBar));
        maybeBaz().ifPresent(maybeBaz -> map.put("maybeBaz", positive.ToMap.add42(maybeBaz)));
        return map;
    }

    public static ToMapConfig.Builder builder() {
        return new ToMapConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        private int foo;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static ToMapConfig.Builder from(ToMap baseConfig) {
            var builder = new ToMapConfig.Builder();
            builder.foo(baseConfig.foo());
            builder.bar(baseConfig.bar());
            builder.baz(baseConfig.baz());
            builder.maybeBar(baseConfig.maybeBar());
            builder.maybeBaz(baseConfig.maybeBaz());
            return builder;
        }

        public ToMapConfig.Builder foo(int foo) {
            this.foo = foo;
            return this;
        }

        public ToMapConfig.Builder bar(long bar) {
            this.config.put("bar", bar);
            return this;
        }

        public ToMapConfig.Builder baz(double baz) {
            this.config.put("baz", baz);
            return this;
        }

        public ToMapConfig.Builder maybeBar(Long maybeBar) {
            this.config.put("maybeBar", maybeBar);
            return this;
        }

        public ToMapConfig.Builder maybeBar(Optional<Long> maybeBar) {
            maybeBar.ifPresent(actualmaybeBar -> this.config.put("maybeBar", actualmaybeBar));
            return this;
        }

        public ToMapConfig.Builder maybeBaz(Double maybeBaz) {
            this.config.put("maybeBaz", maybeBaz);
            return this;
        }

        public ToMapConfig.Builder maybeBaz(Optional<Double> maybeBaz) {
            maybeBaz.ifPresent(actualmaybeBaz -> this.config.put("maybeBaz", actualmaybeBaz));
            return this;
        }

        public ToMap build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new ToMapConfig(foo, config);
        }
    }
}
