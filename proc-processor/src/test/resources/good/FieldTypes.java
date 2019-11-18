package good;

import org.neo4j.graphalgo.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration("FieldTypesConfig")
public interface FieldTypes {

    boolean aBoolean();

    byte aByte();

    short aShort();

    int anInt();

    long aLong();

    float aFloat();

    double aDouble();

    Number aNumber();

    String aString();

    Map<String, Object> aMap();

    List<Object> aList();
}
