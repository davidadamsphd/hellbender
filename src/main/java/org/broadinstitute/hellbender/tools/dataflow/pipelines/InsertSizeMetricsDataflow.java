package org.broadinstitute.hellbender.tools.dataflow.pipelines;

import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.DataFlowProgramGroup;
import org.broadinstitute.hellbender.engine.dataflow.PTransformSAM;
import org.broadinstitute.hellbender.tools.dataflow.transforms.InsertSizeMetricsDataflowTransform;

@CommandLineProgramProperties(summary = "Collect insert size metrics on dataflow" , oneLineSummary = "insert size metrics", programGroup = DataFlowProgramGroup.class)
public class InsertSizeMetricsDataflow extends DataflowReadsPipeline {
    public static final long serialVersionUID = 1l;

    @ArgumentCollection
    InsertSizeMetricsDataflowTransform.Arguments arguments = new InsertSizeMetricsDataflowTransform.Arguments();


    @Override
    protected PTransformSAM<?> getTool() {
        arguments.validate();
        return new InsertSizeMetricsDataflowTransform(arguments);
    }
}
