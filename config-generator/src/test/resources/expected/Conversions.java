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
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class ConversionsConfig implements Conversions.MyConversion {
    private int directMethod;

    private int inheritedMethod;

    private int qualifiedMethod;

    private String referenceTypeAsResult;

    public ConversionsConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.directMethod = Conversions.MyConversion.toInt(config.requireString("directMethod"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedMethod = Conversions.BaseConversion.toIntBase(config.requireString("inheritedMethod"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.qualifiedMethod = Conversions.OtherConversion.toIntQual(config.requireString("qualifiedMethod"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.referenceTypeAsResult = CypherMapWrapper.failOnNull(
                "referenceTypeAsResult",
                Conversions.MyConversion.add42(config.requireString("referenceTypeAsResult"))
            );
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
    public int directMethod() {
        return this.directMethod;
    }

    @Override
    public int inheritedMethod() {
        return this.inheritedMethod;
    }

    @Override
    public int qualifiedMethod() {
        return this.qualifiedMethod;
    }

    @Override
    public String referenceTypeAsResult() {
        return this.referenceTypeAsResult;
    }

    public static ConversionsConfig.Builder builder() {
        return new ConversionsConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static ConversionsConfig.Builder from(Conversions.MyConversion baseConfig) {
            var builder = new ConversionsConfig.Builder();
            builder.directMethod(String.valueOf(baseConfig.directMethod()));
            builder.inheritedMethod(String.valueOf(baseConfig.inheritedMethod()));
            builder.qualifiedMethod(String.valueOf(baseConfig.qualifiedMethod()));
            builder.referenceTypeAsResult(positive.Conversions.MyConversion.remove42(baseConfig.referenceTypeAsResult()));
            return builder;
        }

        public ConversionsConfig.Builder directMethod(String directMethod) {
            this.config.put("directMethod", directMethod);
            return this;
        }

        public ConversionsConfig.Builder inheritedMethod(String inheritedMethod) {
            this.config.put("inheritedMethod", inheritedMethod);
            return this;
        }

        public ConversionsConfig.Builder qualifiedMethod(String qualifiedMethod) {
            this.config.put("qualifiedMethod", qualifiedMethod);
            return this;
        }

        public ConversionsConfig.Builder referenceTypeAsResult(String referenceTypeAsResult) {
            this.config.put("referenceTypeAsResult", referenceTypeAsResult);
            return this;
        }

        public Conversions.MyConversion build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new ConversionsConfig(config);
        }
    }
}
