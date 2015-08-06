package org.broadinstitute.hellbender.tools.spark.examples;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by davidada on 8/6/15.
 */
public class HellbenderRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        System.out.println("*******************************");
        System.out.println("*******************************");
        System.out.println("********registerClasses********");
        System.out.println("*******************************");
        System.out.println("*******************************");
        //kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
    }
}
