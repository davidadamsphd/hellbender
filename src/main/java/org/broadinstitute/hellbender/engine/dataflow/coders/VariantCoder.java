package org.broadinstitute.hellbender.engine.dataflow.coders;


import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.variant.SkeletonVariant;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.broadinstitute.hellbender.utils.variant.VariantContextVariantAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class VariantCoder extends CustomCoder<Variant> {

    @Override
    public void encode( Variant value, OutputStream outStream, Context context ) throws IOException {

        SerializableCoder.of(Class.class).encode(value.getClass(), outStream, context);

        if ( value.getClass() == SkeletonVariant.class ) {
            SerializableCoder.of(SkeletonVariant.class).encode(((SkeletonVariant) value), outStream, context);
        }
        else if (value.getClass() == VariantContextVariantAdapter.class) {
            SerializableCoder.of(VariantContextVariantAdapter.class).encode(((VariantContextVariantAdapter) value), outStream, context);
        } else {
            throw new GATKException("Unknown backing of Variant interface" + value.getClass().getCanonicalName());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Variant decode( InputStream inStream, Context context ) throws IOException {
        final Class clazz = SerializableCoder.of(Class.class).decode(inStream, context);

        if ( clazz == SkeletonVariant.class ) {
            return SerializableCoder.of(SkeletonVariant.class).decode(inStream, context);
        }
        else if ( clazz == VariantContextVariantAdapter.class) {
            return SerializableCoder.of(VariantContextVariantAdapter.class).decode(inStream, context);
        } else {
            throw new GATKException("Unknown backing of Variant interface" + clazz.getCanonicalName());
        }
    }
}
