package dev.suntr.extract.exception;

public class UnSupportedJdbcTypeException extends RuntimeException {
  private static final long serialVersionUID = -4995065568895117744L;

  public UnSupportedJdbcTypeException(String message) {
    super(message);
  }
}
