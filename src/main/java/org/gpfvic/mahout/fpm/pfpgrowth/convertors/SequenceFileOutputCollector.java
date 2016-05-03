package org.gpfvic.mahout.fpm.pfpgrowth.convertors;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Collects the {@link Writable} key and {@link Writable} value, and writes them into a {@link SequenceFile}
 * 
 * @param <K>
 * @param <V>
 */

public class SequenceFileOutputCollector<K extends Writable,V extends Writable> implements
    OutputCollector<K,V> {
  private final SequenceFile.Writer writer;
  
  public SequenceFileOutputCollector(SequenceFile.Writer writer) {
    this.writer = writer;
  }
  
  public final void collect(K key, V value) throws IOException {
    writer.append(key, value);
  }
  
}
