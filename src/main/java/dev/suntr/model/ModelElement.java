package dev.suntr.model;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author suntr
 * @since dmp1.6.0
 * @param <T> 当前节点的数据类型
 *
 */
public class ModelElement<T> implements Serializable {
  private static final long serialVersionUID = -4682118787413913155L;
  private Integer level;

  //每层的数据
  private T element;

  private ModelElement<?> parent;              //父节点
  private List<ModelElement<?>> children;      //子孙节点

  public ModelElement(Integer level) {
    this.level = level;
  }

  public ModelElement(T element) {
    this.element = element;
  }

  public ModelElement(Integer level, T element) {
    this.level = level;
    this.element = element;
  }

  public Integer getLevel() {
    return level;
  }

  protected void setLevel(Integer level) {
    this.level = level;
  }

  public T getElement() {
    return element;
  }

  protected void setElement(T element) {
    this.element = element;
  }

  public ModelElement<?> getParent() {
    return parent;
  }

  protected void setParent(ModelElement<?> parent) {
    this.parent = parent;
  }

  public List<ModelElement<?>> getChildren() {
    return children;
  }

  protected void setChildren(List<ModelElement<?>> children) {
    this.children = children;
  }

  protected void addChild(ModelElement<?> child){
    if (this.children == null){
      this.children = new LinkedList<>();
    }
    this.children.add(child);
  }

  protected void addChildren(List<ModelElement<?>> children){
    if (this.children == null || this.children.size() == 0){
      this.children = children;
    } else {
      this.children.addAll(children);
    }
  }

}