package dev.suntr.extract;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface SqlExtractor {

  List<Map<String, Object>> explain(String sql) throws SQLException;

  List<Object[]> extractArray(String sql) throws SQLException;

  List<Object[]> extractArray(String sql, Object... args) throws SQLException;

  List<Map<String, Object>> extractMap(String sql) throws SQLException;

  List<Map<String, Object>> extractMap(String sql, Object... args) throws SQLException;

}
