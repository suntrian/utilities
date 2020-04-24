package dev.suntr.model;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public class ModelDecorator implements Serializable {
  private static final long serialVersionUID = -8912707087146132642L;

  private int initialMaxLevel = 4;
  public final static String split = "|";

  public static ModelDecoratorBuilder builder(){
    return new ModelDecoratorBuilder();
  }

  //统一保存模型各层级数据的属性方法
  //边的关键信息都保存在了modelEdge中，因此不再保存方法
  private Map<Integer, LevelFunction<Object>> functionMap = new HashMap<>(initialMaxLevel);

  private List<ModelElement<?>> elements;
  private final Map<Integer, List<ModelEdge<?>>> edges = new HashMap<>(initialMaxLevel);
  //游离节点，无法从根节点访问到的节点
  private final Map<Integer, List<ModelElement<?>>> deadElements = new HashMap<>(initialMaxLevel);

  private final List<Integer> functionLevel0Based = new ArrayList<>(initialMaxLevel);
  private final List<Integer> edgeLevel0Based = new ArrayList<>(initialMaxLevel);

  public Map<Integer, LevelFunction<Object>> getFunctionMap() {
    return functionMap;
  }

  protected void setElements(List<ModelElement<?>> elements) {
    this.elements = elements;
  }

  public List<ModelElement<?>> getElements() {
    return elements;
  }

  public Map<Integer, List<ModelEdge<?>>> getEdges() {
    return edges;
  }

  public Map<Integer, List<ModelElement<?>>> getDeadElements() {
    return deadElements;
  }

  public void setFunctionMap(Map<Integer, LevelFunction<?>> functionMap) {
    this.functionMap = (Map) functionMap;
  }

  public boolean isEmpty(){
    return this.elements == null || this.elements.isEmpty();
  }

}

