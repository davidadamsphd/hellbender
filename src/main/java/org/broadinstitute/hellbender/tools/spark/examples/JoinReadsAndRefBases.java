package org.broadinstitute.hellbender.tools.spark.examples;

import com.beust.jcommander.internal.Maps;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPIMetadata;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class JoinReadsAndRefBases {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JoinReadsAndRefBases <apiKey>");
            System.exit(1);
        }
        String bam = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam";

        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsAndRefBases")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "512mb");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<GATKRead> reads = BamLoading.getSerialReads(ctx, bam); // switch back to parallel.
        System.out.println("count: " + reads.count());
        JavaRDD<GATKRead> reads2 = reads.sample(false, 0.1);
        System.out.println("count: " + reads.count());
        /*
        JavaRDD<String> map = reads.map(v1 -> v1.getContig());
        List<String> contigs = map.distinct().collect();
        for (String c : contigs) {
            System.out.println(c);
        }*/
        String referenceName = "EOSt9JOVhp3jkwE";
        Map<String, String> referenceNameToIdTable = Maps.newHashMap();
        referenceNameToIdTable.put("chr1", "EIaSo62VtfXT4AE");
        /*
        Map<String, String> referenceNameToIdTable = RefAPISource.buildReferenceNameToIdTable(options, referenceName);
        System.out.println("===============================");
        System.out.println("===============================");
        System.out.println("===============================");
        for (Map.Entry<String, String> next : referenceNameToIdTable.entrySet()) {
            System.out.println(next.getKey() + ", " + next.getValue());
        }
        System.out.println("===============================");
        System.out.println("===============================");
        System.out.println("===============================");
        */
        RefAPIMetadata refAPIMetadata = new RefAPIMetadata(referenceName, referenceNameToIdTable);


        JavaPairRDD<GATKRead, ReferenceBases> pair = Pair(refAPIMetadata, args[0], reads2);
        Map<GATKRead, ReferenceBases> readRefBases = pair.collectAsMap();
        for (Map.Entry<GATKRead, ReferenceBases> next : readRefBases.entrySet()) {
            System.out.println(next);
        }
    }

    public static JavaPairRDD<GATKRead, ReferenceBases> Pair(RefAPIMetadata refAPIMetadata,
                                                             String apiKey,
                                                             JavaRDD<GATKRead> reads) {

        JavaPairRDD<ReferenceShard, GATKRead> shardRead = reads.mapToPair(gatkRead -> {
            ReferenceShard shard = ReferenceShard.getShardNumberFromInterval(gatkRead);
            return new Tuple2<>(shard, gatkRead);
        });
        List<Tuple2<ReferenceShard, GATKRead>> collect = shardRead.collect();

        JavaPairRDD<ReferenceShard, Iterable<GATKRead>> shardiRead = shardRead.groupByKey();
        //List<Tuple2<ReferenceShard, Iterable<GATKRead>>> collect = shardiRead.collect();
        return shardiRead.flatMapToPair(in -> {
            List<Tuple2<GATKRead, ReferenceBases>> out = Lists.newArrayList();
            Iterable<GATKRead> reads1 = in._2();
            SimpleInterval interval = SimpleInterval.getSpanningInterval(reads1);
            RefAPISource refAPISource = RefAPISource.getRefAPISource();
            ReferenceBases bases = refAPISource.getReferenceBases(apiKey, refAPIMetadata, interval);
            for (GATKRead r : reads1) {
                final ReferenceBases subset = bases.getSubset(new SimpleInterval(r));
                out.add(new Tuple2<>(r, subset));
            }
            return out;
        });
    }
}
