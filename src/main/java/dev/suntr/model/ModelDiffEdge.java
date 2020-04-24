package dev.suntr.model;

import java.util.List;

/**
 * @author suntr
 * @version dmp1.6.0
 * 用于保存边的变更信息
 */
public class ModelDiffEdge {

  private int level;

  //边的变更状态无修改，只有增删
  private final List<ModelEdge<?>> created;
  private final List<ModelEdge<?>> deleted;

  public ModelDiffEdge(int level, List<ModelEdge<?>> created, List<ModelEdge<?>> deleted) {
    this.level = level;
    this.created = created;
    this.deleted = deleted;
  }

  public int getLevel() {
    return level;
  }

  public List<ModelEdge<?>> getCreated() {
    return created;
  }

  public List<ModelEdge<?>> getDeleted() {
    return deleted;
  }
}
