package dev.suntr.extract.entity;

import java.io.Serializable;

/**
 * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
 */

public class RawTable implements Serializable {

  public static final String TYPE_VIEW = "VIEW";
  public static final String TYPE_TABLE = "TABLE";
  public static final String TYPE_SYSTEM = "SYSTEM_TABLE";
  public static final String TYPE_GLOBAL_TEMP = "GLOBAL TEMPORARY";
  public static final String TYPE_LOCAL_TEMP = "LOCAL TEMPORARY";
  public static final String TYPE_ALIAS = "ALIAS";
  public static final String TYPE_SYNONYM = "SYNONYM";

  private String TABLE_CAT;
  private String TABLE_SCHEM;
  private String TABLE_NAME;
  private String TABLE_TYPE;       //"TABLE","VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY","LOCAL TEMPORARY", "ALIAS", "SYNONYM"
  private String REMARKS;
  private String TYPE_CAT;
  private String TYPE_SCHEM;
  private String TYPE_NAME;
  private String SELF_REFERENCING_COL_NAME;
  private String REF_GENERATION;

  public String getTABLE_CAT() {
    return TABLE_CAT;
  }

  public void setTABLE_CAT(String TABLE_CAT) {
    this.TABLE_CAT = TABLE_CAT;
  }

  public String getTABLE_SCHEM() {
    return TABLE_SCHEM;
  }

  public void setTABLE_SCHEM(String TABLE_SCHEM) {
    this.TABLE_SCHEM = TABLE_SCHEM;
  }

  public String getTABLE_NAME() {
    return TABLE_NAME;
  }

  public void setTABLE_NAME(String TABLE_NAME) {
    this.TABLE_NAME = TABLE_NAME;
  }

  public String getTABLE_TYPE() {
    return TABLE_TYPE;
  }

  public void setTABLE_TYPE(String TABLE_TYPE) {
    this.TABLE_TYPE = TABLE_TYPE;
  }

  public String getREMARKS() {
    return REMARKS;
  }

  public void setREMARKS(String REMARKS) {
    this.REMARKS = REMARKS;
  }

  public String getTYPE_CAT() {
    return TYPE_CAT;
  }

  public void setTYPE_CAT(String TYPE_CAT) {
    this.TYPE_CAT = TYPE_CAT;
  }

  public String getTYPE_SCHEM() {
    return TYPE_SCHEM;
  }

  public void setTYPE_SCHEM(String TYPE_SCHEM) {
    this.TYPE_SCHEM = TYPE_SCHEM;
  }

  public String getTYPE_NAME() {
    return TYPE_NAME;
  }

  public void setTYPE_NAME(String TYPE_NAME) {
    this.TYPE_NAME = TYPE_NAME;
  }

  public String getSELF_REFERENCING_COL_NAME() {
    return SELF_REFERENCING_COL_NAME;
  }

  public void setSELF_REFERENCING_COL_NAME(String SELF_REFERENCING_COL_NAME) {
    this.SELF_REFERENCING_COL_NAME = SELF_REFERENCING_COL_NAME;
  }

  public String getREF_GENERATION() {
    return REF_GENERATION;
  }

  public void setREF_GENERATION(String REF_GENERATION) {
    this.REF_GENERATION = REF_GENERATION;
  }
}
