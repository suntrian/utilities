package dev.suntr.extract.entity;

/**
 * @see java.sql.DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)
 */

public class RawIndexInfo {

  private String TABLE_CAT;
  private String TABLE_SCHEM;
  private String TABLE_NAME;
  private Boolean NON_UNIQUE;
  private String INDEX_QUALIFIER;
  private String INDEX_NAME;
  // tableIndexStatistic - this identifies table statistics that are
  // returned in conjuction with a table's index descriptions
  // tableIndexClustered - this is a clustered index
  // tableIndexHashed - this is a hashed index
  // tableIndexOther - this is some other style of index
  private Short TYPE;
  private Short ORDINAL_POSITION;
  private String COLUMN_NAME;
  private String ASC_OR_DESC;
  private Long CARDINALITY;
  private Long PAGES;
  private String FILTER_CONDITION;

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

  public Boolean getNON_UNIQUE() {
    return NON_UNIQUE;
  }

  public void setNON_UNIQUE(Boolean NON_UNIQUE) {
    this.NON_UNIQUE = NON_UNIQUE;
  }

  public String getINDEX_QUALIFIER() {
    return INDEX_QUALIFIER;
  }

  public void setINDEX_QUALIFIER(String INDEX_QUALIFIER) {
    this.INDEX_QUALIFIER = INDEX_QUALIFIER;
  }

  public String getINDEX_NAME() {
    return INDEX_NAME;
  }

  public void setINDEX_NAME(String INDEX_NAME) {
    this.INDEX_NAME = INDEX_NAME;
  }

  public Short getTYPE() {
    return TYPE;
  }

  public void setTYPE(Short TYPE) {
    this.TYPE = TYPE;
  }

  public Short getORDINAL_POSITION() {
    return ORDINAL_POSITION;
  }

  public void setORDINAL_POSITION(Short ORDINAL_POSITION) {
    this.ORDINAL_POSITION = ORDINAL_POSITION;
  }

  public String getCOLUMN_NAME() {
    return COLUMN_NAME;
  }

  public void setCOLUMN_NAME(String COLUMN_NAME) {
    this.COLUMN_NAME = COLUMN_NAME;
  }

  public String getASC_OR_DESC() {
    return ASC_OR_DESC;
  }

  public void setASC_OR_DESC(String ASC_OR_DESC) {
    this.ASC_OR_DESC = ASC_OR_DESC;
  }

  public Long getCARDINALITY() {
    return CARDINALITY;
  }

  public void setCARDINALITY(Long CARDINALITY) {
    this.CARDINALITY = CARDINALITY;
  }

  public Long getPAGES() {
    return PAGES;
  }

  public void setPAGES(Long PAGES) {
    this.PAGES = PAGES;
  }

  public String getFILTER_CONDITION() {
    return FILTER_CONDITION;
  }

  public void setFILTER_CONDITION(String FILTER_CONDITION) {
    this.FILTER_CONDITION = FILTER_CONDITION;
  }
}
