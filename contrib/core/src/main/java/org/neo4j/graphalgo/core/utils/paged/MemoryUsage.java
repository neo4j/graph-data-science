/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.ObjectLongIdentityHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static java.lang.Integer.numberOfTrailingZeros;
import static org.neo4j.graphalgo.core.utils.paged.BitUtil.nextHighestPowerOfTwo;

public final class MemoryUsage {

    private final static int SHIFT_BYTE = numberOfTrailingZeros(Byte.BYTES);
    private final static int SHIFT_CHAR = numberOfTrailingZeros(Character.BYTES);
    private final static int SHIFT_SHORT = numberOfTrailingZeros(Short.BYTES);
    private final static int SHIFT_INT = numberOfTrailingZeros(Integer.BYTES);
    private final static int SHIFT_FLOAT = numberOfTrailingZeros(Float.BYTES);
    private final static int SHIFT_LONG = numberOfTrailingZeros(Long.BYTES);
    private final static int SHIFT_DOUBLE = numberOfTrailingZeros(Double.BYTES);
    private final static int SHIFT_OBJECT_REF;

    public final static int BYTES_OBJECT_REF;

    /**
     * Number of bytes to represent an object header (no fields, no alignments).
     */
    private final static int BYTES_OBJECT_HEADER;

    /**
     * Number of bytes to represent an array header (no content, but with alignments).
     */
    private final static int BYTES_ARRAY_HEADER;

    private final static int MASK1_OBJECT_ALIGNMENT;
    private final static int MASK2_OBJECT_ALIGNMENT;

    /**
     * Sizes of primitive classes.
     */
    private static final ObjectLongMap<Class<?>> primitiveSizes = new ObjectLongIdentityHashMap<>();

    static {
        primitiveSizes.put(boolean.class, 1);
        primitiveSizes.put(byte.class, Byte.BYTES);
        primitiveSizes.put(char.class, Character.BYTES);
        primitiveSizes.put(short.class, Short.BYTES);
        primitiveSizes.put(int.class, Integer.BYTES);
        primitiveSizes.put(float.class, Float.BYTES);
        primitiveSizes.put(double.class, Double.BYTES);
        primitiveSizes.put(long.class, Long.BYTES);
    }

    private static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
    private static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

    /*
     * Initialize constants and try to collect information about the JVM internals.
     */
    static {
        final String osArch = System.getProperty("os.arch");
        final String x = System.getProperty("sun.arch.data.model");
        final boolean is64Bit;
        if (x != null) {
            is64Bit = x.contains("64");
        } else {
            is64Bit = osArch != null && osArch.contains("64");
        }
        if (is64Bit) {
            // Try to get compressed oops and object alignment (the default seems to be 8 on Hotspot);
            // (this only works on 64 bit, on 32 bits the alignment and reference size is fixed):
            boolean compressedOops = false;
            int objectAlignment = 8;
            try {
                final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
                // we use reflection for this, because the management factory is not part
                // of Java 8's compact profile:
                final Object hotSpotBean = Class
                        .forName(MANAGEMENT_FACTORY_CLASS)
                        .getMethod("getPlatformMXBean", Class.class)
                        .invoke(null, beanClazz);
                if (hotSpotBean != null) {
                    final Method getVMOptionMethod = beanClazz.getMethod(
                            "getVMOption",
                            String.class);
                    try {
                        final Object vmOption = getVMOptionMethod.invoke(
                                hotSpotBean,
                                "UseCompressedOops");
                        compressedOops = Boolean.parseBoolean(
                                vmOption
                                        .getClass()
                                        .getMethod("getValue")
                                        .invoke(vmOption)
                                        .toString()
                        );
                    } catch (ReflectiveOperationException | RuntimeException ignored) {
                    }
                    try {
                        final Object vmOption = getVMOptionMethod.invoke(
                                hotSpotBean,
                                "ObjectAlignmentInBytes");
                        objectAlignment = Integer.parseInt(vmOption
                                .getClass()
                                .getMethod("getValue")
                                .invoke(vmOption)
                                .toString());
                        objectAlignment = nextHighestPowerOfTwo(objectAlignment);
                    } catch (ReflectiveOperationException | RuntimeException ignored) {
                    }
                }
            } catch (ReflectiveOperationException | RuntimeException ignored) {
            }
            boolean compressedRefsEnabled = compressedOops;
            int bytesObjectAlignment = objectAlignment;
            MASK1_OBJECT_ALIGNMENT = bytesObjectAlignment - 1;
            MASK2_OBJECT_ALIGNMENT = ~MASK1_OBJECT_ALIGNMENT;
            // reference size is 4, if we have compressed oops:
            BYTES_OBJECT_REF = compressedRefsEnabled ? 4 : 8;
            SHIFT_OBJECT_REF = numberOfTrailingZeros(BYTES_OBJECT_REF);
            // "best guess" based on reference size:
            BYTES_OBJECT_HEADER = 8 + BYTES_OBJECT_REF;
            // array header is NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT, but aligned (object alignment):
            BYTES_ARRAY_HEADER = (int) alignObjectSize(BYTES_OBJECT_HEADER + Integer.BYTES);
        } else {
            int bytesObjectAlignment = 8;
            MASK1_OBJECT_ALIGNMENT = bytesObjectAlignment - 1;
            MASK2_OBJECT_ALIGNMENT = ~MASK1_OBJECT_ALIGNMENT;
            BYTES_OBJECT_REF = 4;
            SHIFT_OBJECT_REF = numberOfTrailingZeros(BYTES_OBJECT_REF);
            BYTES_OBJECT_HEADER = 8;
            // For 32 bit JVMs, no extra alignment of array header:
            BYTES_ARRAY_HEADER = BYTES_OBJECT_HEADER + Integer.BYTES;
        }
    }

    public static long sizeOfByteArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_BYTE));
    }

    public static long sizeOfCharArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_CHAR));
    }

    public static long sizeOfShortArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_SHORT));
    }

    public static long sizeOfIntArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_INT));
    }

    public static long sizeOfFloatArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_FLOAT));
    }

    public static long sizeOfLongArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_LONG));
    }

    public static long sizeOfDoubleArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_DOUBLE));
    }

    public static long sizeOfObjectArray(int length) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_OBJECT_REF));
    }

    public static long sizeOfObjectArrayElements(int length) {
        return alignObjectSize((long) length << SHIFT_OBJECT_REF);
    }

    public static long sizeOfArray(int length, long bytesPerElement) {
        return alignObjectSize((long) BYTES_ARRAY_HEADER + (long) length * bytesPerElement);
    }

    /**
     * Returns the shallow instance size in bytes an instance of the given class would occupy.
     * This works with all conventional classes and primitive types, but not with arrays
     * (the size then depends on the number of elements and varies from object to object).
     *
     * @throws IllegalArgumentException if {@code clazz} is an array class.
     */
    public static long shallowSizeOfInstance(Class<?> clazz) {
        if (clazz.isArray()) {
            throw new IllegalArgumentException(
                    "This method does not work with array classes.");
        }
        if (clazz.isPrimitive()) {
            return primitiveSizes.get(clazz);
        }

        long size = BYTES_OBJECT_HEADER;

        // Walk type hierarchy
        for (; clazz != null; clazz = clazz.getSuperclass()) {
            final Field[] fields = AccessController.doPrivileged((PrivilegedAction<Field[]>) clazz::getDeclaredFields);
            for (Field f : fields) {
                if (!Modifier.isStatic(f.getModifiers())) {
                    size += adjustForField(f);
                }
            }
        }
        return alignObjectSize(size);
    }

    /**
     * Aligns an object size to be the next multiple of object alignment bytes.
     */
    private static long alignObjectSize(long size) {
        return (size + MASK1_OBJECT_ALIGNMENT) & MASK2_OBJECT_ALIGNMENT;
    }

    /**
     * This method returns the maximum representation size of an object. <code>sizeSoFar</code>
     * is the object's size measured so far. <code>f</code> is the field being probed.
     * <p>
     * <p>The returned offset will be the maximum of whatever was measured so far and
     * <code>f</code> field's offset and representation size (unaligned).
     */
    private static long adjustForField(final Field f) {
        final Class<?> type = f.getType();
        if (type.isPrimitive()) {
            return primitiveSizes.get(type);
        }
        return 1 << SHIFT_OBJECT_REF;
    }

    private MemoryUsage() {
        throw new UnsupportedOperationException("No instances");
    }
}
