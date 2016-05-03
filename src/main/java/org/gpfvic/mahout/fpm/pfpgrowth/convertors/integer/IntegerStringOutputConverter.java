package org.gpfvic.mahout.fpm.pfpgrowth.convertors.integer;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.gpfvic.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

/**
 * Collects the Patterns with Integer id and Long support and converts them to Pattern of Strings based on a
 * reverse feature lookup map.
 */

public final class IntegerStringOutputConverter implements
    OutputCollector<Integer,List<Pair<List<Integer>,Long>>> {
  
  private final OutputCollector<Text,TopKStringPatterns> collector;
  
  private final List<String> featureReverseMap;
  
  public IntegerStringOutputConverter(OutputCollector<Text,TopKStringPatterns> collector,
                                      List<String> featureReverseMap) {
    this.collector = collector;
    this.featureReverseMap = featureReverseMap;
  }
  
  public void collect(Integer key, List<Pair<List<Integer>,Long>> value) throws IOException {
    String stringKey = featureReverseMap.get(key);
    List<Pair<List<String>,Long>> stringValues = Lists.newArrayList();
    for (Pair<List<Integer>,Long> e : value) {
      List<String> pattern = Lists.newArrayList();
      for (Integer i : e.getFirst()) {
        pattern.add(featureReverseMap.get(i));
      }
      stringValues.add(new Pair<List<String>,Long>(pattern, e.getSecond()));
    }
    
    collector.collect(new Text(stringKey), new TopKStringPatterns(stringValues));
  }
  
}
