package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Combine.AccumulatingCombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.broadinstitute.hellbender.tools.FlagStat.FlagStatus;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

import java.io.Serializable;

/**
 * Computes Flag stats on a {@link PCollection<MutableGATKRead>}
 */
public final class FlagStatusDataflowTransform extends PTransformSAM<FlagStatus> {
    private static final long serialVersionUID = 1l;

    @Override
    public PCollection<FlagStatus> apply(final PCollection<MutableGATKRead> input) {
        return input.apply(Combine.globally(new CombineCounts()));
    }

    private static class CombineCounts extends AccumulatingCombineFn<MutableGATKRead, StatCounter, FlagStatus> {
        private static final long serialVersionUID = 1l;

        @Override
        public StatCounter createAccumulator() {
            return new StatCounter();
        }
    }

    private static class StatCounter implements Accumulator<MutableGATKRead, StatCounter, FlagStatus>, Serializable {
        private static final long serialVersionUID = 1l;
        private FlagStatus stats = new FlagStatus();

        @Override
        public void addInput(final MutableGATKRead read) {
            stats.add(read);
        }

        @Override
        public void mergeAccumulator(final StatCounter statCounter) {
            stats.merge(statCounter.stats);
        }

        @Override
        public FlagStatus extractOutput() {
            return stats;
        }
    }

}
