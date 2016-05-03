package org.gpfvic.mahout.fpm.pfpgrowth.convertors.string;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;

/**
 * Collects a string pattern in a MaxHeap and outputs the top K patterns
 * 
 */

public final class StringOutputConverter implements OutputCollector<String,List<Pair<List<String>,Long>>> {
  
  private final OutputCollector<Text,TopKStringPatterns> collector;
  
  public StringOutputConverter(OutputCollector<Text,TopKStringPatterns> collector) {
    this.collector = collector;
  }
  
  @Override
  public void collect(String key,
                      List<Pair<List<String>,Long>> value) throws IOException {
    collector.collect(new Text(key), new TopKStringPatterns(value));
  }
}
