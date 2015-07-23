package org.broadinstitute.hellbender.tools.walkers.haplotypecaller;

import org.broadinstitute.hellbender.utils.haplotype.Haplotype;

import java.util.List;
import java.util.Map;

/**
 * Common interface for assembly-haplotype vs reads likelihood engines.
 */
public interface ReadLikelihoodCalculationEngine {

    enum Implementation {
        /**
         * Classic full pair-hmm all haplotypes vs all reads.
         */
        PairHMM,

        /**
         * Graph-base likelihoods.
         */
        GraphBased,

        /**
         * Random likelihoods, used to establish a baseline benchmark for other meaningful implementations.
         */
        Random
    }


    /**
     * Calculates the likelihood of reads across many samples evaluated against haplotypes resulting from the
     * active region assembly process.
     *
     * @param assemblyResultSet the input assembly results.
     * @param samples the list of targeted samples.
     * @param perSampleReadList the input read sets stratified per sample.
     *
     * @throws NullPointerException if either parameter is {@code null}.
     *
     * @return never {@code null}, and with at least one entry for input sample (keys in {@code perSampleReadList}.
     *    The value maps can be potentially empty though.
     */
    public ReadLikelihoods<Haplotype> computeReadLikelihoods(AssemblyResultSet assemblyResultSet, SampleList samples,
                                                             Map<String, List<GATKSAMRecord>> perSampleReadList);

    public void close();
}
