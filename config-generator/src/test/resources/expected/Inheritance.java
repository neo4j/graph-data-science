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
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class MyConfigImpl implements Inheritance.MyConfig {
    private String baseValue;

    private int overriddenValue;

    private long overwrittenValue;

    private double inheritedValue;

    private short inheritedDefaultValue;

    public MyConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.baseValue = CypherMapWrapper.failOnNull("baseValue", config.requireString("baseValue"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.overriddenValue = config.getInt("overriddenValue", Inheritance.MyConfig.super.overriddenValue());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.overwrittenValue = config.getLong("overwrittenValue", Inheritance.MyConfig.super.overwrittenValue());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedValue = config.requireDouble("inheritedValue");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedDefaultValue = config
                .getNumber("inheritedDefaultValue", Inheritance.MyConfig.super.inheritedDefaultValue())
                .shortValue();
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
