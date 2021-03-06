package org.broadinstitute.hellbender.dev.pipelines.bqsr;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import htsjdk.samtools.SAMFileHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.tools.ApplyBQSRArgumentCollection;
import org.broadinstitute.hellbender.transformers.BQSRReadTransformer;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * This transforms applies BQSR to the input reads.
 */
public final class ApplyBQSRTransform extends PTransform<PCollection<GATKRead>, PCollection<GATKRead>> {
    private static final long serialVersionUID = 1L;

    private final SAMFileHeader header;
    private final PCollectionView<BaseRecalOutput> recalView;
    private final ApplyBQSRArgumentCollection args;

    /**
     * @param header The SAM header that corresponds to the reads you're going to pass as input.
     * @param recalibrationOutput the output from BaseRecalibration, with a single BaseRecalOutput object.
     * @param args the recalibration args
     */
    public ApplyBQSRTransform(SAMFileHeader header, PCollection<BaseRecalOutput> recalibrationOutput, ApplyBQSRArgumentCollection args) {
        this.header = header;
        this.recalView = recalibrationOutput.apply(View.asSingleton());
        this.args = args;
    }

    /**
     * @return The same reads as the input, but with updated quality scores.
     */
    @Override
    public PCollection<GATKRead> apply(PCollection<GATKRead> input) {
        return input.apply(ParDo
                .named("ApplyBQSR")
                .withSideInputs(recalView)
                .of(new ApplyBQSR()));
    }

    private class ApplyBQSR extends DoFn<GATKRead, GATKRead> {
        private static final long serialVersionUID = 1L;
        private final transient Logger logger = LogManager.getLogger(ApplyBQSR.class);
        private transient BQSRReadTransformer transformer;

        @Override
        public void processElement(ProcessContext c) {
            if (null==transformer) {
                // set up the transformer, as needed
                BaseRecalOutput info = c.sideInput(recalView);
                transformer = new BQSRReadTransformer(header, info, args.quantizationLevels, args.disableIndelQuals, args.PRESERVE_QSCORES_LESS_THAN, args.emitOriginalQuals, args.globalQScorePrior);
                // it's OK if this same object is used for multiple bundles
            }
            GATKRead r = c.element();
            final GATKRead transformed = transformer.apply(r);
            c.output(transformed);
        }

    }
}


