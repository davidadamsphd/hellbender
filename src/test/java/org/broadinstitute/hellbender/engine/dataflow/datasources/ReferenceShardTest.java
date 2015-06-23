package org.broadinstitute.hellbender.engine.dataflow.datasources;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.readers.bam.ReadConverter;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.MutableGATKRead;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

public final class ReferenceShardTest {

    @DataProvider(name = "reads")
    public Object[][] reads(){
        List<MutableGATKRead> reads = Lists.newArrayList(makeRead(1, 300, 1), makeRead(100000, 10, 2), makeRead(299999, 2, 3));
        List<ReferenceShard> referenceShards = Lists.newArrayList(new ReferenceShard(0, "1"), new ReferenceShard(1, "1"), new ReferenceShard(2, "1"));
        return new Object[][]{
                {reads, referenceShards},
        };
    }

    @DataProvider(name = "refShards")
    public Object[][] refShards(){
        List<ReferenceShard> shards = Lists.newArrayList(new ReferenceShard(0, "1"), new ReferenceShard(1, "1"), new ReferenceShard(2, "1"));
        return new Object[][]{
                {shards},
        };
    }

    public MutableGATKRead makeRead(int start, int length, int i) {
        return ArtificialReadUtils.createRandomRead(start, length, i);
    }

    @Test(dataProvider = "reads")
    public void getVariantShardsFromIntervalTest(List<MutableGATKRead> reads, List<ReferenceShard> shards) {
        for (int i = 0; i < reads.size(); ++i) {
            MutableGATKRead r = reads.get(i);
            ReferenceShard expectedShard = shards.get(i);
            ReferenceShard foundShard = ReferenceShard.getShardNumberFromInterval(r);
            Assert.assertEquals(foundShard, expectedShard);
        }
    }

    @Test(dataProvider = "refShards")
    public void createRefPCollectionTest(List<ReferenceShard> shards) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);

        List<ReferenceShard> shards1 = Lists.newArrayList(shards.iterator());
        Assert.assertEquals(shards, shards1);
        PCollection<ReferenceShard> pVariants = p.apply(Create.of(shards));
        DataflowAssert.that(pVariants).containsInAnyOrder(shards1);

        p.run();
    }
}