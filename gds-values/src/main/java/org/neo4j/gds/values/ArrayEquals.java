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
package org.neo4j.gds.values;

import java.util.Arrays;

/**
 * Static methods for checking the equality of arrays of primitives.
 *
 * This class handles only evaluation of a[] == b[] where type( a ) != type( b ), ei. byte[] == int[] and such.
 * byte[] == byte[] evaluation can be done using Arrays.equals().
 */
public final class ArrayEquals {
    private ArrayEquals() {}

    // TYPED COMPARISON

    public static boolean byteAndShort(byte[] a, short[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    public static boolean byteAndInt(byte[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndLong(byte[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndFloat(byte[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndDouble(byte[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndInt(short[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndLong(short[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndFloat(short[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndDouble(short[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndLong(int[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndFloat(int[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndDouble(int[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean longAndFloat(long[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean longAndDouble(long[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean floatAndDouble(float[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    // NON-TYPED COMPARISON

    public static boolean byteAndObject(byte[] a, Object b) {
        if (b instanceof byte[]) {
            return Arrays.equals(a, (byte[])b);
        } else if (b instanceof short[]) {
            return byteAndShort(a, (short[])b);
        } else if (b instanceof int[]) {
            return byteAndInt(a, (int[])b);
        } else if (b instanceof long[]) {
            return byteAndLong(a, (long[])b);
        } else if (b instanceof float[]) {
            return byteAndFloat(a, (float[])b);
        } else {
            return b instanceof double[] ? byteAndDouble(a, (double[])b) : false;
        }
    }

    public static boolean shortAndObject(short[] a, Object b) {
        if (b instanceof byte[]) {
            return byteAndShort((byte[])b, a);
        } else if (b instanceof short[]) {
            return Arrays.equals(a, (short[])b);
        } else if (b instanceof int[]) {
            return shortAndInt(a, (int[])b);
        } else if (b instanceof long[]) {
            return shortAndLong(a, (long[])b);
        } else if (b instanceof float[]) {
            return shortAndFloat(a, (float[])b);
        } else {
            return b instanceof double[] ? shortAndDouble(a, (double[])b) : false;
        }
    }

    public static boolean intAndObject(int[] a, Object b) {
        if (b instanceof byte[]) {
            return byteAndInt((byte[])b, a);
        } else if (b instanceof short[]) {
            return shortAndInt((short[])b, a);
        } else if (b instanceof int[]) {
            return Arrays.equals(a, (int[])b);
        } else if (b instanceof long[]) {
            return intAndLong(a, (long[])b);
        } else if (b instanceof float[]) {
            return intAndFloat(a, (float[])b);
        } else {
            return b instanceof double[] ? intAndDouble(a, (double[])b) : false;
        }
    }

    public static boolean longAndObject(long[] a, Object b) {
        if (b instanceof byte[]) {
            return byteAndLong((byte[])b, a);
        } else if (b instanceof short[]) {
            return shortAndLong((short[])b, a);
        } else if (b instanceof int[]) {
            return intAndLong((int[])b, a);
        } else if (b instanceof long[]) {
            return Arrays.equals(a, (long[])b);
        } else if (b instanceof float[]) {
            return longAndFloat(a, (float[])b);
        } else {
            return b instanceof double[] ? longAndDouble(a, (double[])b) : false;
        }
    }

    public static boolean floatAndObject(float[] a, Object b) {
        if (b instanceof byte[]) {
            return byteAndFloat((byte[])b, a);
        } else if (b instanceof short[]) {
            return shortAndFloat((short[])b, a);
        } else if (b instanceof int[]) {
            return intAndFloat((int[])b, a);
        } else if (b instanceof long[]) {
            return longAndFloat((long[])b, a);
        } else if (b instanceof float[]) {
            return Arrays.equals(a, (float[])b);
        } else {
            return b instanceof double[] ? floatAndDouble(a, (double[])b) : false;
        }
    }

    public static boolean doubleAndObject(double[] a, Object b) {
        if (b instanceof byte[]) {
            return byteAndDouble((byte[])b, a);
        } else if (b instanceof short[]) {
            return shortAndDouble((short[])b, a);
        } else if (b instanceof int[]) {
            return intAndDouble((int[])b, a);
        } else if (b instanceof long[]) {
            return longAndDouble((long[])b, a);
        } else if (b instanceof float[]) {
            return floatAndDouble((float[])b, a);
        } else {
            return b instanceof double[] ? Arrays.equals(a, (double[])b) : false;
        }
    }
}
