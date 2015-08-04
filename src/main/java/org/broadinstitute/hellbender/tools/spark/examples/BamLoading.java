/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.broadinstitute.hellbender.tools.spark.examples;

import com.beust.jcommander.internal.Lists;
import com.cloudera.dataflow.hadoop.HadoopIO;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.File;
import java.util.List;

// Try with:
// src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam
public final class BamLoading {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: BamLoading <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("BamLoading");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String bam = args[0];

        final SAMFileHeader readsHeader = getHeader(bam);
        List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        final SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);

        ReadsDataSource bam2 = new ReadsDataSource(new File(bam), samReaderFactory);
        bam2.setIntervalsForTraversal(intervals);
        List<GATKRead> records = Lists.newArrayList();
        for ( GATKRead read : bam2 ) {
            records.add(read);
        }

        JavaRDD<GATKRead> rddReads = ctx.parallelize(records);
        long count = rddReads.count();

        JavaPairRDD<LongWritable, SAMRecordWritable> rdd2 = ctx.newAPIHadoopFile(
                bam, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class,
                new Configuration());
        long count1 = rdd2.count();
        System.out.println("counts: " + count + ", " + count1);


        /*
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */
        ctx.stop();
    }

    private static SAMFileHeader getHeader(String bam) {
        return SamReaderFactory.makeDefault().getFileHeader(new File(bam));
    }
}