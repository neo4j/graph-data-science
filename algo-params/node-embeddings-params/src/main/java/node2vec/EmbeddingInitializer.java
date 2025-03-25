/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package node2vec;

import org.neo4j.gds.utils.StringJoining;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public enum EmbeddingInitializer {
    UNIFORM,
    NORMALIZED;

    private static final List<String> VALUES = Arrays
        .stream(EmbeddingInitializer.values())
        .map(EmbeddingInitializer::name)
        .collect(Collectors.toList());

    public static EmbeddingInitializer parse(Object input) {
        if (input instanceof String) {
            var inputString = toUpperCaseWithLocale((String) input);

            if (!VALUES.contains(inputString)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "EmbeddingInitializer `%s` is not supported. Must be one of: %s.",
                    input,
                    StringJoining.join(VALUES)
                ));
            }

            return valueOf(toUpperCaseWithLocale(inputString));
        } else if (input instanceof EmbeddingInitializer) {
            return (EmbeddingInitializer) input;
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected EmbeddingInitializer or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(EmbeddingInitializer embeddingInitializer) {
        return embeddingInitializer.toString();
    }
}
