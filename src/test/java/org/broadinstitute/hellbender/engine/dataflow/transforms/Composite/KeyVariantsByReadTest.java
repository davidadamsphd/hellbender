package org.broadinstitute.hellbender.engine.dataflow.transforms.composite;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.DataflowTestData;
import org.broadinstitute.hellbender.engine.dataflow.DataflowTestUtils;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public final class KeyVariantsByReadTest {

    @DataProvider(name = "variantsAndReads")
    public Object[][] variantsAndReads(){
        DataflowTestData testData = new DataflowTestData();

        List<MutableGATKRead> reads = testData.getReads();
        List<Variant> variantList = testData.getVariants();
        List<KV<MutableGATKRead, Iterable<Variant>>> kvReadiVariant = testData.getKvReadiVariant();

        return new Object[][]{
                {reads, variantList, kvReadiVariant},
        };
    }

    @Test(dataProvider = "variantsAndReads")
    public void addContextDataTest(List<MutableGATKRead> reads, List<Variant> variantList,
                                   List<KV<MutableGATKRead, Iterable<Variant>>> kvReadiVariant) {
        Pipeline p = TestPipeline.create();
        DataflowUtils.registerGATKCoders(p);

        PCollection<MutableGATKRead> pReads = DataflowTestUtils.PCollectionCreateAndVerify(p, reads);
        PCollection<Variant> pVariant = DataflowTestUtils.PCollectionCreateAndVerify(p, variantList);

        PCollection<KV<MutableGATKRead, Iterable<Variant>>> result = KeyVariantsByRead.Key(pVariant, pReads);
        DataflowAssert.that(result).containsInAnyOrder(kvReadiVariant);
        p.run();
    }
}