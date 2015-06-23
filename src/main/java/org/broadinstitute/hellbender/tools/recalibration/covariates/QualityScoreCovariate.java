package org.broadinstitute.hellbender.tools.recalibration.covariates;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.tools.recalibration.ReadCovariates;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationArgumentCollection;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

/**
 * The Reported Quality Score covariate.
 */
public final class QualityScoreCovariate implements Covariate {

    public QualityScoreCovariate(final RecalibrationArgumentCollection RAC){
        //nothing to initialize
    }

    @Override
    public void recordValues(final MutableGATKRead read, final SAMFileHeader header, final ReadCovariates values) {
        final byte[] baseQualities = read.getBaseQualities();
        final byte[] baseInsertionQualities = ReadUtils.getBaseInsertionQualities(read);
        final byte[] baseDeletionQualities = ReadUtils.getBaseDeletionQualities(read);

        for (int i = 0; i < baseQualities.length; i++) {
            values.addCovariate(baseQualities[i], baseInsertionQualities[i], baseDeletionQualities[i], i);
        }
    }

    @Override
    public String formatKey(final int key) {
        return String.format("%d", key);
    }

    @Override
    public int keyFromValue(final Object value) {
        return (value instanceof String) ? Byte.parseByte((String) value) : (int)(Byte) value;
    }

    @Override
    public int maximumKeyValue() {
        return QualityUtils.MAX_SAM_QUAL_SCORE;
    }
}