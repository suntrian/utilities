package dev.suntr.extract.entity;

import java.sql.DatabaseMetaData;

/**
 * @see DatabaseMetaData#getCatalogs()
 * @see DatabaseMetaData#getSchemas()
 */
public class RawDatabase {
  private String TABLE_CAT;
  private String TABLE_SCHEM;

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
}
