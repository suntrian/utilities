package dev.suntr.extract.entity;

import java.io.Serializable;

/**
 * @see java.sql.DatabaseMetaData#getColumns(String, String, String, String)
 *  <OL>
 *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
 *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
 *  <LI><B>TABLE_NAME</B> String {@code =>} table name
 *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
 *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
 *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
 *  for a UDT the type name is fully qualified
 *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
 *  <LI><B>BUFFER_LENGTH</B> is not used.
 *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
 * DECIMAL_DIGITS is not applicable.
 *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
 *  <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
 *      <UL>
 *      <LI> columnNoNulls - might not allow <code>NULL</code> values
 *      <LI> columnNullable - definitely allows <code>NULL</code> values
 *      <LI> columnNullableUnknown - nullability unknown
 *      </UL>
 *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
 *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
 *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
 *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
 *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
 *       maximum number of bytes in the column
 *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
 *      (starting at 1)
 *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
 *       <UL>
 *       <LI> YES           --- if the column can include NULLs
 *       <LI> NO            --- if the column cannot include NULLs
 *       <LI> empty string  --- if the nullability for the
 * column is unknown
 *       </UL>
 *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
 *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
 *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
 *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
 *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
 *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
 *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
 *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
 *      isn't DISTINCT or user-generated REF)
 *   <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
 *       <UL>
 *       <LI> YES           --- if the column is auto incremented
 *       <LI> NO            --- if the column is not auto incremented
 *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
 *       </UL>
 *   <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
 *       <UL>
 *       <LI> YES           --- if this a generated column
 *       <LI> NO            --- if this not a generated column
 *       <LI> empty string  --- if it cannot be determined whether this is a generated column
 *       </UL>
 *  </OL>
 */
public class RawColumn implements Serializable {
  private String TABLE_CAT;
  private String TABLE_SCHEM;
  private String TABLE_NAME;
  private String COLUMN_NAME;
  private Integer DATA_TYPE;
  private String TYPE_NAME;
  private Integer COLUMN_SIZE;
  private Integer BUFFER_LENGTH;   //not used
  private Integer DECIMAL_DIGITS;
  private Integer NUM_PREC_RADIX;
  private Integer NULLABLE;       //0 nonnull, 1 nullable, 2 unknown
  private String REMARKS;
  private String COLUMN_DEF;
  private Integer SQL_DATA_TYPE; //unused
  private Integer SQL_DATETIME_SUB;   //unused
  private Integer CHAR_OCTET_LENGTH;
  private Integer ORDINAL_POSITION;
  private String IS_NULLABLE;     //YES for nullable, NO for nonnull, empty string for unknown
  private String SCOPE_CATALOG;
  private String SCOPE_SCHEMA;
  private String SCOPE_TABLE;
  private Short SOURCE_DATA_TYPE;
  private String IS_AUTOINCREMENT; //YES for yes, NO for non, empty string for unknown
  private String IS_GENERATEDCOLUMN; //YES for yes, NO for non, empty string for unknown

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

  public Integer getDATA_TYPE() {
    return DATA_TYPE;
  }

  public void setDATA_TYPE(Integer DATA_TYPE) {
    this.DATA_TYPE = DATA_TYPE;
  }

  public String getTYPE_NAME() {
    return TYPE_NAME;
  }

  public void setTYPE_NAME(String TYPE_NAME) {
    this.TYPE_NAME = TYPE_NAME;
  }

  public Integer getCOLUMN_SIZE() {
    return COLUMN_SIZE;
  }

  public void setCOLUMN_SIZE(Integer COLUMN_SIZE) {
    this.COLUMN_SIZE = COLUMN_SIZE;
  }

  public Integer getBUFFER_LENGTH() {
    return BUFFER_LENGTH;
  }

  public void setBUFFER_LENGTH(Integer BUFFER_LENGTH) {
    this.BUFFER_LENGTH = BUFFER_LENGTH;
  }

  public Integer getDECIMAL_DIGITS() {
    return DECIMAL_DIGITS;
  }

  public void setDECIMAL_DIGITS(Integer DECIMAL_DIGITS) {
    this.DECIMAL_DIGITS = DECIMAL_DIGITS;
  }

  public Integer getNUM_PREC_RADIX() {
    return NUM_PREC_RADIX;
  }

  public void setNUM_PREC_RADIX(Integer NUM_PREC_RADIX) {
    this.NUM_PREC_RADIX = NUM_PREC_RADIX;
  }

  public Integer getNULLABLE() {
    return NULLABLE;
  }

  public void setNULLABLE(Integer NULLABLE) {
    this.NULLABLE = NULLABLE;
  }

  public String getREMARKS() {
    return REMARKS;
  }

  public void setREMARKS(String REMARKS) {
    this.REMARKS = REMARKS;
  }

  public String getCOLUMN_DEF() {
    return COLUMN_DEF;
  }

  public void setCOLUMN_DEF(String COLUMN_DEF) {
    this.COLUMN_DEF = COLUMN_DEF;
  }

  public Integer getSQL_DATA_TYPE() {
    return SQL_DATA_TYPE;
  }

  public void setSQL_DATA_TYPE(Integer SQL_DATA_TYPE) {
    this.SQL_DATA_TYPE = SQL_DATA_TYPE;
  }

  public Integer getSQL_DATETIME_SUB() {
    return SQL_DATETIME_SUB;
  }

  public void setSQL_DATETIME_SUB(Integer SQL_DATETIME_SUB) {
    this.SQL_DATETIME_SUB = SQL_DATETIME_SUB;
  }

  public Integer getCHAR_OCTET_LENGTH() {
    return CHAR_OCTET_LENGTH;
  }

  public void setCHAR_OCTET_LENGTH(Integer CHAR_OCTET_LENGTH) {
    this.CHAR_OCTET_LENGTH = CHAR_OCTET_LENGTH;
  }

  public Integer getORDINAL_POSITION() {
    return ORDINAL_POSITION;
  }

  public void setORDINAL_POSITION(Integer ORDINAL_POSITION) {
    this.ORDINAL_POSITION = ORDINAL_POSITION;
  }

  public String getIS_NULLABLE() {
    return IS_NULLABLE;
  }

  public void setIS_NULLABLE(String IS_NULLABLE) {
    this.IS_NULLABLE = IS_NULLABLE;
  }

  public String getSCOPE_CATALOG() {
    return SCOPE_CATALOG;
  }

  public void setSCOPE_CATALOG(String SCOPE_CATALOG) {
    this.SCOPE_CATALOG = SCOPE_CATALOG;
  }

  public String getSCOPE_SCHEMA() {
    return SCOPE_SCHEMA;
  }

  public void setSCOPE_SCHEMA(String SCOPE_SCHEMA) {
    this.SCOPE_SCHEMA = SCOPE_SCHEMA;
  }

  public String getSCOPE_TABLE() {
    return SCOPE_TABLE;
  }

  public void setSCOPE_TABLE(String SCOPE_TABLE) {
    this.SCOPE_TABLE = SCOPE_TABLE;
  }

  public Short getSOURCE_DATA_TYPE() {
    return SOURCE_DATA_TYPE;
  }

  public void setSOURCE_DATA_TYPE(Short SOURCE_DATA_TYPE) {
    this.SOURCE_DATA_TYPE = SOURCE_DATA_TYPE;
  }

  public String getIS_AUTOINCREMENT() {
    return IS_AUTOINCREMENT;
  }

  public void setIS_AUTOINCREMENT(String IS_AUTOINCREMENT) {
    this.IS_AUTOINCREMENT = IS_AUTOINCREMENT;
  }

  public String getIS_GENERATEDCOLUMN() {
    return IS_GENERATEDCOLUMN;
  }

  public void setIS_GENERATEDCOLUMN(String IS_GENERATEDCOLUMN) {
    this.IS_GENERATEDCOLUMN = IS_GENERATEDCOLUMN;
  }
}
