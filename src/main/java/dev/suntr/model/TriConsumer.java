package dev.suntr.model;

import java.io.Serializable;
import java.util.Objects;

@FunctionalInterface
public interface TriConsumer<T1, T2, T3> extends Serializable {

  void accept(T1 t1, T2 t2, T3 t3);

  default TriConsumer<T1,T2,T3> andThen(TriConsumer<? super T1, ? super T2, ? super T3> after) {
    Objects.requireNonNull(after);
    return (T1 t1, T2 t2, T3 t3) -> { accept(t1, t2,t3); after.accept(t1,t2,t3); };
  }
}
