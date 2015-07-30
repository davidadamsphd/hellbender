package org.broadinstitute.hellbender.engine.dataflow.datasources;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;

public class RefWindowFunctions {

    public static final SerializableFunction<GATKRead, SimpleInterval> IDENTITY_FUNCTION = read -> new SimpleInterval(read);

    public static final class FixedWindowFunction implements SerializableFunction<GATKRead, SimpleInterval> {
        private static final long serialVersionUID = 1L;

        private final int leadingWindowBases;
        private final int trailingWindowBases;

        public FixedWindowFunction( final int leadingWindowBases, final int trailingWindowBases ) {
            this.leadingWindowBases = leadingWindowBases;
            this.trailingWindowBases = trailingWindowBases;
        }

        @Override
        public SimpleInterval apply( GATKRead read ) {
            // TODO: truncate interval at contig end
            return new SimpleInterval(read.getContig(), Math.max(read.getStart() - leadingWindowBases, 1), read.getEnd() + trailingWindowBases);
        }
    }
}
