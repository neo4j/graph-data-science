package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.jetbrains.annotations.Nullable;

import java.io.Flushable;
import java.io.IOException;

import static org.neo4j.graphalgo.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.api.DefaultValue.LONG_DEFAULT_FALLBACK;

public interface FileAppender extends Flushable, AutoCloseable {

    void append(long value) throws IOException;

    void append(double value) throws IOException;

    void append(float[] value) throws IOException;

    void append(long[] value) throws IOException;

    void append(double[] value) throws IOException;

    default void appendAny(@Nullable Object value) throws IOException {
        if (value instanceof Double) {
            var doubleValue = (double) value;
            if (!Double.isNaN(doubleValue)) {
                append(doubleValue);
            } else {
                appendEmptyField();
            }
        } else if (value instanceof Long) {
            var longValue = (long) value;
            if (longValue != LONG_DEFAULT_FALLBACK && longValue != INTEGER_DEFAULT_FALLBACK) {
                append(longValue);
            } else {
                appendEmptyField();
            }
        } else if (value instanceof double[]) {
            append((double[]) value);
        } else if (value instanceof long[]) {
            append((long[]) value);
        } else if (value instanceof float[]) {
            append((float[]) value);
        } else if (value == null) {
            appendEmptyField();
        }
    }

    void append(String value) throws IOException;

    void appendEmptyField() throws IOException;

    void startLine() throws IOException;

    void endLine() throws IOException;

    @Override
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
