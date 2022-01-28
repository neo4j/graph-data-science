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
package org.neo4j.gds.proc;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.auto.common.MoreTypes.isTypeOf;

final class GenerateAuxiliaryMethods {

    private GenerateAuxiliaryMethods() {}

    static void injectToMapCode(ConfigParser.Spec config, MethodSpec.Builder builder) {
        List<ConfigParser.Member> configMembers = config
            .members()
            .stream()
            .filter(ConfigParser.Member::isConfigMapEntry)
            .collect(Collectors.toList());

        switch (configMembers.size()) {
            case 0:
                builder.addStatement("return $T.emptyMap()", Collections.class);
                break;
            case 1:
                ConfigParser.Member singleConfigMember = configMembers.get(0);
                String parameter = singleConfigMember.lookupKey();
                builder.addStatement(
                    "return $T.singletonMap($S, $L)",
                    Collections.class,
                    parameter,
                    getMapValueCode(singleConfigMember)
                );
                break;
            default:
                builder.addStatement("$T<$T, Object> map = new $T<>()", Map.class, String.class, LinkedHashMap.class);
                configMembers.forEach(configMember -> {
                    if (isTypeOf(Optional.class, configMember.method().getReturnType())) {
                        builder.addStatement(getMapPutOptionalCode(configMember));
                    } else {
                        builder.addStatement(
                            "map.put($S, $L)",
                            configMember.lookupKey(),
                            getMapValueCode(configMember)
                        );
                    }
                });
                builder.addStatement("return map");
                break;
        }
    }

    @NotNull
    private static CodeBlock getMapValueCode(ConfigParser.Member configMember) {
        String getter = configMember.methodName();
        Configuration.ToMapValue toMapValue = configMember.method().getAnnotation(Configuration.ToMapValue.class);
        return (toMapValue == null)
            ? CodeBlock.of("$N()", getter)
            : CodeBlock.of("$L($N())", GenerateAuxiliaryMethods.getReference(toMapValue), getter);
    }

    @NotNull
    private static CodeBlock getMapPutOptionalCode(ConfigParser.Member configMember) {
        Configuration.ToMapValue toMapValue = configMember.method().getAnnotation(Configuration.ToMapValue.class);

        CodeBlock mapValue = (toMapValue == null)
            ? CodeBlock.of("$L", configMember.lookupKey())
            : CodeBlock.of("$L($L)", GenerateAuxiliaryMethods.getReference(toMapValue), configMember.lookupKey());

        return CodeBlock.of("$L.ifPresent($L -> map.put($S, $L))",
            CodeBlock.of("$N()", configMember.methodName()),
            configMember.lookupKey(),
            configMember.lookupKey(),
            mapValue
        );
    }

    private static String getReference(Configuration.ToMapValue toMapValue) {
        return toMapValue.value().replace("#", ".");
    }

    static CodeBlock collectKeysCode(ConfigParser.Spec config) {
        Collection<String> configKeys = config
            .members()
            .stream()
            .filter(ConfigParser.Member::isConfigMapEntry)
            .map(ConfigParser.Member::lookupKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        switch (configKeys.size()) {
            case 0:
                return CodeBlock.of("return $T.emptyList()", Collections.class);
            case 1:
                return CodeBlock.of("return $T.singleton($S)", Collections.class, configKeys.iterator().next());
            default:
                CodeBlock keys = configKeys
                    .stream()
                    .map(name -> CodeBlock.of("$S", name))
                    .collect(CodeBlock.joining(", "));
                return CodeBlock.of("return $T.asList($L)", Arrays.class, keys);
        }
    }

    static void injectGraphStoreValidationCode(
        ConfigParser.Member validationMethod,
        ConfigParser.Spec config,
        MethodSpec.Builder builder
    ) {
        List<ConfigParser.Member> validationChecks = config
            .members()
            .stream()
            .filter(ConfigParser.Member::graphStoreValidationCheck)
            .collect(Collectors.toList());

        var parameters = validationMethod.method().getParameters();

        validationChecks
            .stream()
            .map(check -> CodeBlock.of(
                "$N($N, $N, $N)",
                check.methodName(),
                parameters.get(0).getSimpleName(),
                parameters.get(1).getSimpleName(),
                parameters.get(2).getSimpleName()
            ))
            .forEach(builder::addStatement);
    }
}
