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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class CollectingKeysConfig implements CollectingKeys {
    private int foo;

    private long bar;

    private double baz;

    public CollectingKeysConfig(int foo, @NotNull CypherMapWrapper config) {
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
    public Collection<String> configKeys() {
        return Arrays.asList("bar", "baz");
    }

    public static CollectingKeysConfig.Builder builder() {
        return new CollectingKeysConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        private int foo;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static CollectingKeysConfig.Builder from(CollectingKeys baseConfig) {
            var builder = new CollectingKeysConfig.Builder();
            builder.foo(baseConfig.foo());
            builder.bar(baseConfig.bar());
            builder.baz(baseConfig.baz());
            return builder;
        }

        public CollectingKeysConfig.Builder foo(int foo) {
            this.foo = foo;
            return this;
        }

        public CollectingKeysConfig.Builder bar(long bar) {
            this.config.put("bar", bar);
            return this;
        }

        public CollectingKeysConfig.Builder baz(double baz) {
            this.config.put("baz", baz);
            return this;
        }

        public CollectingKeys build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new CollectingKeysConfig(foo, config);
        }
    }
}
