package org.gpfvic.mahout.fpm.pfpgrowth;

import java.io.IOException;

import org.apache.mahout.math.list.IntArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;

/**
 *  takes each group of dependent transactions and\ compacts it in a
 * TransactionTree structure
 */

public class ParallelFPGrowthCombiner extends Reducer<IntWritable,TransactionTree,IntWritable,TransactionTree> {
  
  @Override
  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Context context)
    throws IOException, InterruptedException {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair<IntArrayList,Long> p : tr) {
        cTree.addPattern(p.getFirst(), p.getSecond());
      }
    }
    context.write(key, cTree.getCompressedTree());
  }
  
}
