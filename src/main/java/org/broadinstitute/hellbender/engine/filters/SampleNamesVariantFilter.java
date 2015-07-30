package org.broadinstitute.hellbender.engine.filters;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.HashSet;
import java.util.Set;

/**
 * Keep only variants with any of these sample names.
 * Matching is done by case-sensitive exact match.
 */
public final class SampleNamesVariantFilter implements VariantFilter {
    private static final long serialVersionUID = 1L;

    private final HashSet<String> sampleNames = new HashSet<String>();

    public SampleNamesVariantFilter (Set<String> includeNames) {
        Utils.nonNull(includeNames);
        sampleNames.addAll(includeNames);
    }

    @Override
    public boolean test( final VariantContext vc ) {
        final Set<String> vcSampleNames = vc.getSampleNames();
        return vcSampleNames.stream().anyMatch(s -> sampleNames.contains(s));
    }
}
