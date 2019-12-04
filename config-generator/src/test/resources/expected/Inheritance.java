/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class MyConfig implements Inheritance.MyConfig {

    private final String baseValue;

    private final int overriddenValue;

    private final long overwrittenValue;

    private final double inheritedValue;

    private final short inheritedDefaultValue;

    public MyConfig(@NotNull CypherMapWrapper config) {
        this.baseValue = CypherMapWrapper.failOnNull("baseValue", config.requireString("baseValue"));
        this.overriddenValue = config.getInt("overriddenValue", Inheritance.MyConfig.super.overriddenValue());
        this.overwrittenValue = config.getLong("overwrittenValue", Inheritance.MyConfig.super.overwrittenValue());
        this.inheritedValue = config.requireDouble("inheritedValue");
        this.inheritedDefaultValue = config
            .getNumber("inheritedDefaultValue", Inheritance.MyConfig.super.inheritedDefaultValue())
            .shortValue();
    }

    public MyConfig(
        @NotNull String baseValue,
        int overriddenValue,
        long overwrittenValue,
        double inheritedValue,
        short inheritedDefaultValue
    ) {
        this.baseValue = CypherMapWrapper.failOnNull("baseValue", baseValue);
        this.overriddenValue = overriddenValue;
        this.overwrittenValue = overwrittenValue;
        this.inheritedValue = inheritedValue;
        this.inheritedDefaultValue = inheritedDefaultValue;
    }

    @Override
    public String baseValue() {
        return this.baseValue;
    }

    @Override
    public int overriddenValue() {
        return this.overriddenValue;
    }

    @Override
    public long overwrittenValue() {
        return this.overwrittenValue;
    }

    @Override
    public double inheritedValue() {
        return this.inheritedValue;
    }

    @Override
    public short inheritedDefaultValue() {
        return this.inheritedDefaultValue;
    }
}
