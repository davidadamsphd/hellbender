package org.broadinstitute.hellbender.tools;

import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;

@CommandLineProgramProperties(
	usage = "Walks over the input data set, calculating the number of bases seen for diagnostic purposes.",
	usageShort = "Count bases",
    programGroup = ReadProgramGroup.class
)
public final class CountBases extends ReadWalker {

    private long count = 0;

    @Override
    public void apply( MutableGATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        count += read.getLength();
    }

    @Override
    public Object onTraversalDone() {
        return count;
    }
}
