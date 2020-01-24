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
package positive;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ToMapConfig implements ToMap {

    private final int foo;

    private final long bar;

    private final double baz;

    public ToMapConfig(int foo, @NotNull CypherMapWrapper config) {
        this.foo = foo;
        this.bar = config.requireLong("bar");
        this.baz = config.requireDouble("baz");
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
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("bar", bar());
        map.put("baz", baz());
        return map;
    }
}
