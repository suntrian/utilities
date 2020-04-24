package dev.suntr.extract.entity;

import java.util.Date;

public class RawRoutine {

  private String ROUTINE_CAT;
  private String ROUTINE_SCHEMA;
  private String ROUTINE_NAME;
  private String ROUTINE_TYPE;
  private String ROUTINE_DEFINITION;
  private String ROUTINE_PARAMS;
  private String ROUTINE_RETURNS;
  private Date CREATED;
  private Date UPDATED;

  public String getROUTINE_CAT() {
    return ROUTINE_CAT;
  }

  public void setROUTINE_CAT(String ROUTINE_CAT) {
    this.ROUTINE_CAT = ROUTINE_CAT;
  }

  public String getROUTINE_SCHEMA() {
    return ROUTINE_SCHEMA;
  }

  public void setROUTINE_SCHEMA(String ROUTINE_SCHEMA) {
    this.ROUTINE_SCHEMA = ROUTINE_SCHEMA;
  }

  public String getROUTINE_NAME() {
    return ROUTINE_NAME;
  }

  public void setROUTINE_NAME(String ROUTINE_NAME) {
    this.ROUTINE_NAME = ROUTINE_NAME;
  }

  public String getROUTINE_TYPE() {
    return ROUTINE_TYPE;
  }

  public void setROUTINE_TYPE(String ROUTINE_TYPE) {
    this.ROUTINE_TYPE = ROUTINE_TYPE;
  }

  public String getROUTINE_DEFINITION() {
    return ROUTINE_DEFINITION;
  }

  public void setROUTINE_DEFINITION(String ROUTINE_DEFINITION) {
    this.ROUTINE_DEFINITION = ROUTINE_DEFINITION;
  }

  public String getROUTINE_PARAMS() {
    return ROUTINE_PARAMS;
  }

  public void setROUTINE_PARAMS(String ROUTINE_PARAMS) {
    this.ROUTINE_PARAMS = ROUTINE_PARAMS;
  }

  public String getROUTINE_RETURNS() {
    return ROUTINE_RETURNS;
  }

  public void setROUTINE_RETURNS(String ROUTINE_RETURNS) {
    this.ROUTINE_RETURNS = ROUTINE_RETURNS;
  }

  public Date getCREATED() {
    return CREATED;
  }

  public void setCREATED(Date CREATED) {
    this.CREATED = CREATED;
  }

  public Date getUPDATED() {
    return UPDATED;
  }

  public void setUPDATED(Date UPDATED) {
    this.UPDATED = UPDATED;
  }
}
