package org.broadinstitute.hellbender.engine.dataflow.transforms;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;


public class GoogleGenomicsReadToGATKRead extends PTransform<PCollection<Read>, PCollection<MutableGATKRead>> {

    @Override
    public PCollection<MutableGATKRead> apply( PCollection<Read> input ) {
        return input.apply(ParDo.of(new DoFn<Read, MutableGATKRead>() {
            @Override
            public void processElement( ProcessContext c ) throws Exception {
                c.output(new GoogleGenomicsReadToGATKReadAdapter(c.element()));
            }
        })); //.setCoder(GoogleGenomicsReadToGATKReadAdapter.CODER);
    }
}