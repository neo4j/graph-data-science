package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;
import java.util.List;
import java.util.Map;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class FieldTypesConfig implements FieldTypes {

    private final boolean aBoolean;

    private final byte aByte;

    private final short aShort;

    private final int anInt;

    private final long aLong;

    private final float aFloat;

    private final double aDouble;

    private final Number aNumber;

    private final String aString;

    private final Map<String, Object> aMap;

    private final List<Object> aList;

    public FieldTypesConfig(CypherMapWrapper config) {
        this.aBoolean = config.requireBool("aBoolean");
        this.aByte = config.requireNumber("aByte").byteValue();
        this.aShort = config.requireNumber("aShort").shortValue();
        this.anInt = config.requireInt("anInt");
        this.aLong = config.requireLong("aLong");
        this.aFloat = config.requireNumber("aFloat").floatValue();
        this.aDouble = config.requireDouble("aDouble");
        this.aNumber = config.requireNumber("aNumber");
        this.aString = config.requireString("aString");
        this.aMap = config.requireChecked("aMap", Map.class);
        this.aList = config.requireChecked("aList", List.class);
    }

    @Override
    public boolean aBoolean() {
        return this.aBoolean;
    }

    @Override
    public byte aByte() {
        return this.aByte;
    }

    @Override
    public short aShort() {
        return this.aShort;
    }

    @Override
    public int anInt() {
        return this.anInt;
    }

    @Override
    public long aLong() {
        return this.aLong
    }

    @Override
    public float aFloat() {
        return this.aFloat;
    }

    @Override
    public double aDouble() {
        return this.aDouble;
    }

    @Override
    public Number aNumber() {
        return this.aNumber;
    }

    @Override
    public String aString() {
        return this.aString;
    }

    @Override
    public Map<String, Object> aMap() {
        return this.aMap;
    }

    @Override
    public List<Object> aList() {
        return this.aList;
    }
}
