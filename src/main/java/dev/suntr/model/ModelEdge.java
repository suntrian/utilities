package dev.suntr.model;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author suntr
 * @since dmp1.6.0
 * @param <PK>
 */
public class ModelEdge<PK> implements Serializable{
  private static final long serialVersionUID = 3194366723002292779L;

  private PK source;
  private PK target;
  private Serializable extra;

  protected <E> ModelEdge(E edge, Function<E, PK> getSource, Function<E, PK> getTarget){
    this.source = getSource.apply(edge);
    this.target =getTarget.apply(edge);
  }

  protected<E> ModelEdge(E edge, Function<E, PK> getSource, Function<E, PK> getTarget, Function<E, ? extends Serializable> getExtra){
    this.source = getSource.apply(edge);
    this.target =getTarget.apply(edge);
    this.extra = getExtra.apply(edge);
  }

  protected ModelEdge(PK source, PK target) {
    this.source = source;
    this.target = target;
  }

  protected ModelEdge(PK source, PK target, Serializable extra) {
    this.source = source;
    this.target = target;
    this.extra = extra;
  }

  public PK getSource() {
    return source;
  }

  protected void setSource(PK source) {
    this.source = source;
  }

  public PK getTarget() {
    return target;
  }

  protected void setTarget(PK target) {
    this.target = target;
  }

  public Serializable getExtra() {
    return extra;
  }

  protected void setExtra(Serializable extra) {
    this.extra = extra;
  }
}