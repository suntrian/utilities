package dev.suntr.model;

import java.io.Serializable;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class LevelFunction<T> implements Serializable {

  private static final long serialVersionUID = -2565599590654233367L;
  
  Function<T, ? extends Comparable<?>> keyGetter;
  
  Function<T, ? extends Comparable<?>> parentGetter;
  
  Function<T, ?>[] propertyGetter;

  public LevelFunction<T> setKeyGetter(Function<T, ? extends Comparable<?>> keyGetter) {
    this.keyGetter =  keyGetter;
    return this;
  }

  public LevelFunction<T> setParentGetter(Function<T, ? extends Comparable<?>> parentGetter) {
    this.parentGetter = parentGetter;
    return this;
  }

  @SafeVarargs
  public final LevelFunction<T> setPropertyGetter(Function<T, ? extends Serializable>... propertyGetter) {
    this.propertyGetter = propertyGetter;
    return this;
  }

  public Function<T, ? extends Comparable<?>> getKeyGetter() {
    return keyGetter;
  }

  public Function<T, ? extends Comparable<?>> getParentGetter() {
    return parentGetter;
  }

  public Function<T, ?>[] getPropertyGetter() {
    return propertyGetter;
  }
}

