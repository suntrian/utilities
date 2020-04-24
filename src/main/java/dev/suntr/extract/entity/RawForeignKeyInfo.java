package dev.suntr.extract.entity;

/**
 * @see java.sql.DatabaseMetaData#getExportedKeys(String, String, String)
 * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
 */

public class RawForeignKeyInfo {

    public enum Rule{
        UNKNOWN ("UNKNOWN", 99),
        CASCADE("CASCADE", 0),
        RESTRICT("RESTRICT", 1),
        SET_NULL("SET_NULL", 2),
        NO_ACTION( "NO_ACTION", 3),
        SET_DEFAULT("SET_DEFAULT", 4);
        private String name;
        private Integer code;

        Rule(String name, Integer code) {
            this.name = name;
            this.code = code;
        }

        public static Rule of(int rule){
            for (Rule r: Rule.values()){
                if (r.code == rule){
                    return r;
                }
            }
            return UNKNOWN;
        }
    }

    private String PKTABLE_CAT;
    private String PKTABLE_SCHEM;
    private String PKTABLE_NAME;
    private String PKCOLUMN_NAME;
    private String FKTABLE_CAT;
    private String FKTABLE_SCHEM;
    private String FKTABLE_NAME;
    private String FKCOLUMN_NAME;
    private Short  KEY_SEQ;
    private Short  UPDATE_RULE;
    private Short  DELETE_RULE;
    private String FK_NAME;
    private String PK_NAME;

    public String getPKTABLE_CAT() {
        return PKTABLE_CAT;
    }

    public void setPKTABLE_CAT(String PKTABLE_CAT) {
        this.PKTABLE_CAT = PKTABLE_CAT;
    }

    public String getPKTABLE_SCHEM() {
        return PKTABLE_SCHEM;
    }

    public void setPKTABLE_SCHEM(String PKTABLE_SCHEM) {
        this.PKTABLE_SCHEM = PKTABLE_SCHEM;
    }

    public String getPKTABLE_NAME() {
        return PKTABLE_NAME;
    }

    public void setPKTABLE_NAME(String PKTABLE_NAME) {
        this.PKTABLE_NAME = PKTABLE_NAME;
    }

    public String getPKCOLUMN_NAME() {
        return PKCOLUMN_NAME;
    }

    public void setPKCOLUMN_NAME(String PKCOLUMN_NAME) {
        this.PKCOLUMN_NAME = PKCOLUMN_NAME;
    }

    public String getFKTABLE_CAT() {
        return FKTABLE_CAT;
    }

    public void setFKTABLE_CAT(String FKTABLE_CAT) {
        this.FKTABLE_CAT = FKTABLE_CAT;
    }

    public String getFKTABLE_SCHEM() {
        return FKTABLE_SCHEM;
    }

    public void setFKTABLE_SCHEM(String FKTABLE_SCHEM) {
        this.FKTABLE_SCHEM = FKTABLE_SCHEM;
    }

    public String getFKTABLE_NAME() {
        return FKTABLE_NAME;
    }

    public void setFKTABLE_NAME(String FKTABLE_NAME) {
        this.FKTABLE_NAME = FKTABLE_NAME;
    }

    public String getFKCOLUMN_NAME() {
        return FKCOLUMN_NAME;
    }

    public void setFKCOLUMN_NAME(String FKCOLUMN_NAME) {
        this.FKCOLUMN_NAME = FKCOLUMN_NAME;
    }

    public Short getKEY_SEQ() {
        return KEY_SEQ;
    }

    public void setKEY_SEQ(Short KEY_SEQ) {
        this.KEY_SEQ = KEY_SEQ;
    }

    public Short getUPDATE_RULE() {
        return UPDATE_RULE;
    }

    public void setUPDATE_RULE(Short UPDATE_RULE) {
        this.UPDATE_RULE = UPDATE_RULE;
    }

    public Short getDELETE_RULE() {
        return DELETE_RULE;
    }

    public void setDELETE_RULE(Short DELETE_RULE) {
        this.DELETE_RULE = DELETE_RULE;
    }

    public String getFK_NAME() {
        return FK_NAME;
    }

    public void setFK_NAME(String FK_NAME) {
        this.FK_NAME = FK_NAME;
    }

    public String getPK_NAME() {
        return PK_NAME;
    }

    public void setPK_NAME(String PK_NAME) {
        this.PK_NAME = PK_NAME;
    }
}
