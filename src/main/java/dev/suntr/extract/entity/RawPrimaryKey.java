package dev.suntr.extract.entity;

/**
 * @see java.sql.DatabaseMetaData#getPrimaryKeys(String, String, String)
 */

public class RawPrimaryKey {

  private String TABLE_CAT;
  private String TABLE_SCHEM;
  private String TABLE_NAME;
  private String COLUMN_NAME;
  private Short KEY_SEQ;
  private String PK_NAME;

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

  public String getCOLUMN_NAME() {
    return COLUMN_NAME;
  }

  public void setCOLUMN_NAME(String COLUMN_NAME) {
    this.COLUMN_NAME = COLUMN_NAME;
  }

  public Short getKEY_SEQ() {
    return KEY_SEQ;
  }

  public void setKEY_SEQ(Short KEY_SEQ) {
    this.KEY_SEQ = KEY_SEQ;
  }

  public String getPK_NAME() {
    return PK_NAME;
  }

  public void setPK_NAME(String PK_NAME) {
    this.PK_NAME = PK_NAME;
  }
}
