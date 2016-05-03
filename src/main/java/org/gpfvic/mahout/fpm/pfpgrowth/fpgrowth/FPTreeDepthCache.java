package org.gpfvic.mahout.fpm.pfpgrowth.fpgrowth;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Caches large FPTree {@link Object} for each level of the recursive
 * {@link FPGrowth} algorithm to reduce allocation overhead.
 */

public class FPTreeDepthCache {

  private final LeastKCache<Integer,FPTree> firstLevelCache = new LeastKCache<Integer,FPTree>(5);
  private int hits;
  private int misses;
  private final List<FPTree> treeCache = Lists.newArrayList();
  
  public final FPTree getFirstLevelTree(Integer attr) {
    FPTree tree = firstLevelCache.get(attr);
    if (tree != null) {
      hits++;
      return tree;
    } else {
      misses++;
      FPTree conditionalTree = new FPTree();
      firstLevelCache.set(attr, conditionalTree);
      return conditionalTree;
    }
  }
  
  public final int getHits() {
    return hits;
  }
  
  public final int getMisses() {
    return misses;
  }
  
  public final FPTree getTree(int level) {
    while (treeCache.size() < level + 1) {
      FPTree cTree = new FPTree();
      treeCache.add(cTree);
    }
    FPTree conditionalTree = treeCache.get(level);
    conditionalTree.clear();
    return conditionalTree;
  }
  
}
