package org.broadinstitute.hellbender.engine.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;
import htsjdk.samtools.SAMRecord;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.engine.dataflow.transforms.transformExample;
import org.broadinstitute.hellbender.engine.dataflow.transforms.transformExample2;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SampleUnitTest {

    @DataProvider(name = "refAndInts")
    public Object[][] refAndInts(){
        Integer[] integers = new Integer[]{1, 2, 3, 4};
        Set<KV<ReferenceShard, Integer>> s = new HashSet<>();
        s.add(KV.of(new ReferenceShard(1, "1"), 1));
        s.add(KV.of(new ReferenceShard(2, "2"), 2));
        s.add(KV.of(new ReferenceShard(3, "3"), 3));
        s.add(KV.of(new ReferenceShard(4, "4"), 4));

        return new Object[][]{
                { integers, s}
        };
    }

    @DataProvider(name = "refs")
    public Object[][] refs(){
        ReferenceShard[] integers = new ReferenceShard[]{
                new ReferenceShard(1, "1"),
                new ReferenceShard(2, "2"),
                new ReferenceShard(3, "3"),
                new ReferenceShard(4, "4")
        };
        Set<ReferenceShard> s = new HashSet<>();
        s.add(new ReferenceShard(1, "1"));
        s.add(new ReferenceShard(2, "2"));
        s.add(new ReferenceShard(3, "3"));
        s.add(new ReferenceShard(4, "4"));

        return new Object[][]{
                { integers, s}
        };
    }

    static class GroupIntsDoFn extends DoFn<Integer, KV<ReferenceShard, Integer>> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            Integer i = c.element();
            c.output(KV.of(new ReferenceShard(i.intValue(), i.toString()), i));
        }
    }

    static class IterableRefShardsDoFn extends DoFn<ReferenceShard, Iterable<ReferenceShard>> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            ReferenceShard i = c.element();
            c.output(Lists.newArrayList(i, i, i));
        }
    }

    @Test(dataProvider = "refAndInts")
    public void GroupIntsTest(Integer[] integers, Set<KV<ReferenceShard, Integer>> s) {
        DoFnTester<Integer, KV<ReferenceShard, Integer>> fnTester = DoFnTester.of(new GroupIntsDoFn());
        List<KV<ReferenceShard, Integer>> kvs = fnTester.processBatch(integers);
        for (KV<ReferenceShard, Integer> r : kvs) {
            Assert.assertTrue(s.contains(r));
        }
    }

    @Test(dataProvider = "refs")
    public void anotherDoFnTest(ReferenceShard[] referenceShards, Set<ReferenceShard> s) {
        DoFnTester<ReferenceShard, Iterable<ReferenceShard>> fnTester = DoFnTester.of(new IterableRefShardsDoFn());
        List<Iterable<ReferenceShard>> kvs = fnTester.processBatch(referenceShards);

        Assert.assertEquals(referenceShards.length, kvs.size());
        for (Iterable<ReferenceShard> intz : kvs) {
            int size = 0;
            for (ReferenceShard i : intz) {
                size++;
                Assert.assertTrue(s.contains(i));
            }
            Assert.assertEquals(3, size);
        }
    }

    @Test(dataProvider = "refs")
    public void createRefsTest(ReferenceShard[] referenceShards, Set<ReferenceShard> s) {
        // The simplist way to figure out if a class is coded correctly is to create a PCollection
        // of that type and see if matches the List version.
        Pipeline p = TestPipeline.create();
        // We don't need to call:
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);
        // because we use the DefaultCoder annotation on the class.
        List<ReferenceShard> shards = Lists.newArrayList(referenceShards);
        List<ReferenceShard> shards2 = Lists.newArrayList(referenceShards);
        PCollection<ReferenceShard> pShards = p.apply(Create.of(shards));
        DataflowAssert.that(pShards).containsInAnyOrder(shards2);
        p.run();
    }

    @Test(dataProvider = "refAndInts")
    public void transformExampleTest(Integer[] integers, Set<KV<ReferenceShard, Integer>> s) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);

        List<Integer> ints = Lists.newArrayList(integers);
        PCollection<Integer> pInts = p.apply(Create.of(ints));

        // Note that we needed to make the Transform a full class (not anonymous).
        // So, you can't test a PTransform by writing it within a test.
        PCollection<KV<ReferenceShard, Integer>> keyedInts =
                pInts.apply(new transformExample());
        List<KV<ReferenceShard, Integer>> e = Lists.newArrayList(s);
        DataflowAssert.that(keyedInts).containsInAnyOrder(e);

        p.run();
    }

    @Test(dataProvider = "refAndInts")
    public void transformExampleTest2(Integer[] integers, Set<KV<ReferenceShard, Integer>> s) {
        Pipeline p = TestPipeline.create();
        p.getCoderRegistry().registerCoder(ReferenceShard.class, ReferenceShard.CODER);

        List<Integer> ints = Lists.newArrayList(integers);
        PCollection<Integer> pInts = p.apply(Create.of(ints));

        // Note that we needed to make the Transform a full class (not anonymous).
        // So, you can't test a PTransform by writing it within a test.
        PCollection<String> keyedInts2 =
                pInts.apply(new transformExample2());
        List<String> e = Lists.newArrayList();
        for (Integer i : integers) {
            e.add(i.toString());
        }
        DataflowAssert.that(keyedInts2).containsInAnyOrder(e);

        p.run();
    }
}
