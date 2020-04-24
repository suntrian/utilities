package dev.suntr.excel;

import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author suntri
 * @since dmp 1.6.0
 * 暂只支持输出pojo中的string, int ,double, long, date, boolean 等简单数据类型
 */
@SuppressWarnings("unchecked")
public class ExcelReader {

  private int sheetCount = 1;
  private final int emptyRowExitThreshold = 100;
  private boolean isXlsx = true;

  private Workbook workbook;
  private FormulaEvaluator formulaEvaluator;
  private POIFSFileSystem poifsFileSystem;
  private OPCPackage opcPackage;

  private List<Class> columnClass;
  private List<List<String>> excelTitles;
  private List<List<String>> excelColumns;
  private List<Map<String, Method>> classSetters;
  //每个sheet的标题行数，读取内容时跳过标题行
  private List<Integer> skipRows;
  //限制输出多少行的内容
  private List<Integer> limitRows;
  //当有多行标题时，指定匹配的标题行
  private List<Integer> titleRows;
  //输出的sheet, 1 based
  private Set<Integer> sheetRead = new HashSet<>();
  //输出的列，KEY:EXCEL中的列序号0based, VALUE：输入的列序号0based
  private List<Map<Integer, Integer>> columnMap;
  private Map<String, Map<String, Object>> sheetNameMap = new HashMap<>();
  private List<String> sheetNames;

  //此时间日期判断的正则表达式比较简单，没有判断更复杂的情况如2019-14-29 等也能匹配
  private Pattern datetimePattern = Pattern.compile("(?:(?<year>(?:[1-9]\\d)?\\d{2})[/-])?(?<mon>[0-1]?\\d)[/-](?<day>[0-3]?\\d)\\s+(?<hour>[0-2]?\\d):(?<min>[0-5]?\\d)(?::(?<sec>[0-5]?\\d))?");
  private Pattern datePattern = Pattern.compile("((?<year>(?:[1-9]\\d)?\\d{2})[/-])?(?<mon>[0-1]?\\d)[/-](?<day>[0-3]?\\d)");
  private Pattern timePattern = Pattern.compile("(?<hour>[0-2]?\\d):(?<min>[0-5]?\\d)(?::(?<sec>[0-5]?\\d))?");

  public ExcelReader() {
  }

  public ExcelReader(File excel) throws IOException, InvalidFormatException {
    init(excel);
  }

  public ExcelReader setClass(Class<?> clazz){
    this.columnClass = setListValue(this.columnClass, -1, clazz, null, sheetCount);
    return this;
  }

  public ExcelReader setClass(int sheetNum, Class<?> clazz){
    this.sheetRead.add(sheetNum);
    this.columnClass = setListValue(this.columnClass, sheetNum-1, clazz, null, sheetCount);
    return this;
  }
  public ExcelReader setClass(String sheetName, Class<?> clazz){
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("class", clazz);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  public ExcelReader setSkipRows(int skipRows){
    this.skipRows = setListValue(this.skipRows, -1, skipRows, 0, sheetCount);
    return this;
  }

  public ExcelReader setSkipRows(int sheetNum, int skipRows){
    this.sheetRead.add(sheetNum);
    this.skipRows = setListValue(this.skipRows, sheetNum-1, skipRows, 0, sheetCount);
    return this;
  }
  public ExcelReader setSkipRows(String sheetName, int skipRows){
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("skipRows", skipRows);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  public ExcelReader setLimitRows(int sheetNum, int limitRows){
    this.sheetRead.add(sheetNum);
    this.limitRows = setListValue(this.limitRows, sheetNum-1, limitRows, 0, sheetCount);
    return this;
  }
  public ExcelReader setLimitRows(String sheetName, int limitRows){
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("limitRows", limitRows);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  public ExcelReader setLimitRows(int limitRows){
    this.limitRows = setListValue(this.limitRows, -1, limitRows, 0, sheetCount);
    return this;
  }

  public ExcelReader setTitleRow(int sheetNum, int titleRow){
    this.sheetRead.add(sheetNum);
    this.titleRows = setListValue(this.titleRows, sheetNum-1, titleRow, 0, sheetCount);
    return this;
  }

  public ExcelReader setTitleRow(int titleRow){
    this.titleRows = setListValue(this.titleRows, -1, titleRow, 0, sheetCount);
    return this;
  }

  public ExcelReader setTitleRow(String sheetName, int titleRow){
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("titleRows", titleRow);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  public ExcelReader setTitles(int sheetNum, List<String> titles){
    this.sheetRead.add(sheetNum);
    this.excelTitles = setListValue(this.excelTitles, sheetNum-1, titles, Collections.emptyList(), sheetCount);
    return this;
  }
  public ExcelReader setTitles(String sheetName, List<String> titles){
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("titles", titles);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  public ExcelReader setTitles(String... titles){
    return setTitles(Arrays.asList(titles));
  }

  public ExcelReader setTitles(List<String> titles){
    this.excelTitles = setListValue(this.excelTitles, -1, titles, Collections.emptyList(), sheetCount);
    return this;
  }

  /**
   * 设置要读取的sheet, 不设置则默认读取全部
   * @param sheetNum num
   * @return
   */
  public ExcelReader setSheetRead(Integer sheetNum){
    if (this.sheetRead == null){
      this.sheetRead = new HashSet<>(sheetCount);
    }
    this.sheetRead.add(sheetNum);
    return this;
  }

  public ExcelReader setSheetRead(Integer... sheetNums){
    if (this.sheetRead == null){
      this.sheetRead = new HashSet<>(sheetCount);
    }
    this.sheetRead.addAll(Arrays.asList(sheetNums));
    return this;
  }

  public ExcelReader setSortedFields(String... columns){
    return setSortedFields(Arrays.asList(columns));
  }

  public ExcelReader setSortedFields(List<String> columns){
    return setSortedFields(-1, columns);
  }
  /**
   *
   * @param columns  excel输出的列属性名称
   * @param sheetNum  base 1
   * @return this
   */
  public ExcelReader setSortedFields(int sheetNum, List<String> columns) {
    this.sheetRead.add(sheetNum);
    this.excelColumns = setListValue(this.excelColumns, sheetNum-1, columns, Collections.emptyList(), sheetCount);
    return this;
  }
  public ExcelReader setSortedFields(String sheetName, List<String> columns) {
    Map<String, Object> map = sheetNameMap.getOrDefault(sheetName, new HashMap<>());
    map.put("columns", columns);
    sheetNameMap.put(sheetName, map);
    return this;
  }

  private int countSheet() {
    return this.workbook.getNumberOfSheets();
  }

  public ResultSet read(File file) throws IOException, InvalidFormatException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    init(file);
    return parse();
  }

  public ResultSet read() throws NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, InvocationTargetException {
    if (this.workbook == null) {
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    return parse();
  }

  /**
   * use more memory then read from file or filechannel
   * @param inputStream
   * @return
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   */
  public ResultSet read(InputStream inputStream) throws InvocationTargetException,
      NoSuchMethodException, InstantiationException, IllegalAccessException, IOException {
    init(inputStream);
    return parse();
  }

  public <T> Stream<T> streamOf(File file) throws IOException, InvalidFormatException, NoSuchMethodException {
    init(file);
    if (sheetCount == 1){
      return parseToStream(1);
    } else if (sheetCount>1 &&  (this.sheetRead !=null && this.sheetRead.size() == 1)){
      int sheetToRead = this.sheetRead.iterator().next();
      return parseToStream(sheetToRead);
    }
    throw new IllegalArgumentException("unSupport to read dual sheet by Stream");
  }

  public <T> Stream<T> streamOf(int sheetNum1Based) throws NoSuchMethodException, IllegalAccessException {
    if (this.workbook == null) {
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    return parseToStream(sheetNum1Based);
  }

  public <T> Stream<T> streamOf(String sheetNamePattern) throws IllegalAccessException, NoSuchMethodException {
    if (this.workbook == null) {
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    for (int i = 0 ; i < this.workbook.getNumberOfSheets(); i++){
      if (this.workbook.getSheetName(i).matches(sheetNamePattern)){
        return parseToStream(i+1);
      }
    }
    throw new IllegalStateException("can not find a sheet name matches " + sheetNamePattern);
  }

  public List<String> getSheetNames() throws IllegalAccessException {
    if (this.workbook == null) {
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    return Stream.iterate(0, i->i+1).limit(this.workbook.getNumberOfSheets()).map(i->this.workbook.getSheetName(i)).collect(Collectors.toList());
  }

  public List<String> getTitles(int sheetIndex1Based, int titleRow) throws IllegalAccessException {
    if (this.workbook == null){
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    if (sheetIndex1Based < 1 || sheetIndex1Based-1>=this.workbook.getNumberOfSheets()){
      return Collections.emptyList();
    }
    return getStringRow(this.workbook.getSheetAt(sheetIndex1Based-1).getRow( titleRow));
  }

  public List<String> getTitles(String sheetNamePattern, int titleRow) throws IllegalAccessException {
    if (this.workbook == null){
      throw new IllegalAccessException("Use Constructor or read method for an excelfile");
    }
    return Stream.iterate(0, i->i+1)
            .limit(this.workbook.getNumberOfSheets())
            .filter(i->this.workbook.getSheetName(i).matches(sheetNamePattern))
            .map(i->this.workbook.getSheetAt(i))
            .map(i->i.getRow(titleRow))
            .map(this::getStringRow)
            .findFirst()
            .orElse(Collections.emptyList());
  }

  private void init(File file) throws IOException, InvalidFormatException {
    String suffix = fileExtension(file.getName()).toLowerCase();
    if (!(suffix.equals("xls") || suffix.equals("xlsx") || suffix.equals("xlsm") )){
      throw new IllegalArgumentException("not excel extension");
    }
    switch (FileMagic.valueOf(file)){
      case OLE2:
        this.isXlsx = false;
        this.poifsFileSystem = new POIFSFileSystem(file, true);
        this.workbook = new HSSFWorkbook(poifsFileSystem);
        break;
      case OOXML:
        this.isXlsx = true;
        this.opcPackage = OPCPackage.open(file, PackageAccess.READ);
        this.workbook = new XSSFWorkbook(this.opcPackage); // new SXSSFWorkbook(new XSSFWorkbook(this.opcPackage));  //SXSSFWorkbook 仅用于写文件，无法读取文件
        break;
      default:
        throw new IllegalArgumentException("not excel extension");
    }
  }

  private void init(InputStream inputStream) throws IOException {
    inputStream = FileMagic.prepareToCheckMagic(inputStream);
    switch (FileMagic.valueOf(inputStream)){
      case OLE2:
        this.isXlsx = false;
        this.workbook = new HSSFWorkbook(inputStream);
        break;
      case OOXML:
        this.isXlsx = true;
        this.workbook = new XSSFWorkbook(inputStream);  //new SXSSFWorkbook(new XSSFWorkbook(inputStream));  //SXSSFWorkbook 仅用于写文件，无法读取文件
        break;
      default:
        throw new IllegalArgumentException("not excel extension");
    }
  }

  private void preHandler() throws NoSuchMethodException {
    int actualSheetCount = countSheet();

    this.sheetNames = new ArrayList<>(actualSheetCount);
    for (int i = 0; i < actualSheetCount; i++){
      this.sheetNames.add(this.workbook.getSheetName(i));
    }

    if (this.sheetNameMap.size()>0){
      for (String sName: this.sheetNameMap.keySet()){
        for (int i = 0; i< actualSheetCount; i++){
          if (sheetNames.get(i).matches(sName.trim())) {
          //if (sName.trim().equalsIgnoreCase(sheetNames.get(i))){
            Map<String, Object> settings = this.sheetNameMap.get(sName);
            this.sheetRead.add(i+1);
            this.columnClass = this.setListValue(this.columnClass, i, (Class)settings.getOrDefault("class", null), null, this.sheetCount);
            this.excelTitles = this.setListValue(this.excelTitles, i, (List<String>) settings.getOrDefault("titles", Collections.emptyList()), Collections.emptyList(), this.sheetCount);
            this.excelColumns = this.setListValue(this.excelColumns,i, (List<String>) settings.getOrDefault("columns", Collections.emptyList()), Collections.emptyList(), this.sheetCount);
            this.skipRows = this.setListValue(this.skipRows, i, (int)settings.getOrDefault("skipRows", 0), 0, this.sheetCount);
            this.limitRows = this.setListValue(this.limitRows, i, (int)settings.getOrDefault("limitRows", 0), 0, this.sheetCount);
            this.titleRows = this.setListValue(this.titleRows, i, (int)settings.getOrDefault("titleRows", 0), 0, this.sheetCount);
            break;
          }
        }
      }
    }

    if (this.sheetRead==null || this.sheetRead.size() == 0){
      this.sheetRead = new HashSet<>(actualSheetCount);
      if (this.columnClass!=null) {for (int i = 1; i <= this.columnClass.size(); i++){sheetRead.add(i);}}
      if (this.excelColumns!=null){for (int i = 1; i <= this.excelColumns.size(); i++){sheetRead.add(i);}}
    }
    this.sheetCount = this.sheetRead.size();

    if (this.sheetCount == 0 || actualSheetCount < this.sheetCount){
      this.sheetCount = actualSheetCount;
    }

    for (int i = 0; i < this.sheetCount; i++){
      Class clazz;
      //要读取的标题名称
      List<String> exportTitles = getListValue(this.excelTitles, i, Collections.emptyList());
      if (exportTitles.size()>0){
        //EXCEL的标题名称
        int titleRow = getListValue(this.titleRows, i, 0);
        List<String> titles = getStringRow(this.workbook.getSheetAt(i).getRow(titleRow));
        Map<Integer,Integer> exportCols = new HashMap<>(exportTitles.size());
        for (int m = 0; m < exportTitles.size(); m++){
          if (exportTitles.get(m) == null || "".equals(exportTitles.get(m).trim())){ continue;}
          for (int n = 0; n < titles.size(); n++){
            if (titles.get(n) == null || "".equals(titles.get(n).trim())) {continue;}
            if (titles.get(n).trim().matches(exportTitles.get(m).trim())) {
            //if (exportTitles.get(m).trim().toUpperCase().equals(titles.get(n).trim().toUpperCase())){
              exportCols.put(n, m);
              break;
            }
          }
        }
        if (exportCols.size() == 0) {
          throw new IllegalStateException("未找到指定的数据列,请检查文件是否符合模板");
        }
        this.columnMap = setListValue(this.columnMap, i,exportCols, Collections.emptyMap(), this.sheetCount);
      }
      if ( (clazz = getListValue(this.columnClass, i, null)) == null) {
        this.columnClass = setListValue(this.columnClass, i, ArrayList.class, ArrayList.class, this.sheetCount);
      } else if (List.class.isAssignableFrom(clazz)){
        this.columnClass = setListValue(this.columnClass, i, clazz, clazz, this.sheetCount);
      } else if (Map.class.isAssignableFrom(clazz)) {
          //如果输出map且存在标题行，且没有设置输出的属性名的话，默认以标题行作为KEY
        if (getListValue(this.excelColumns, i, Collections.emptyList()).size() == 0) {
          int titleRows;
          if ((titleRows = getListValue(this.skipRows, i, 0)) == 0) {
            throw new IllegalStateException("please set one title rows or field names");
          }
          else if (titleRows == 1) {
            //需检查是否存在数据
            Row titleRow = this.workbook.getSheetAt(i).getRow(0);
            List<String> titles = getStringRow(titleRow);
            this.excelColumns = setListValue(this.excelColumns, i, titles, Collections.emptyList(), sheetCount);
          } else {
            throw new IllegalStateException("more than one title row exists");
          }
        }
      } else {
        List<String> columns;
        if ((columns = getListValue(this.excelColumns, i, Collections.emptyList())).size() == 0) {
          throw new IllegalStateException("未指定输出列的属性");
        }
        List<Method> methods = BeanUtil.getBeanSetters(clazz);
        Map<String, Method> methodMap = new HashMap<>(columns.size());
        OUT:
        for (String col : columns) {
          if (col == null || "".equals(col.trim())) {
            continue;
          }
          Iterator<Method> methodIter = methods.iterator();
          while (methodIter.hasNext()) {
            Method method = methodIter.next();
            if (method.getName().substring(3).equals(BeanUtil.capture(col))) {
              methodMap.put(col, method);
              methodIter.remove();
              continue OUT;
            }
          }
          throw new NoSuchMethodException("未找到方法:" + col);
        }
        this.classSetters = setListValue(this.classSetters, i, methodMap, Collections.emptyMap(), sheetCount);
      }
    }
  }

  private List<String> getStringRow(Row row){
    List<String> list = new ArrayList<>(row.getLastCellNum());
    for (int l = 0; l < row.getLastCellNum(); l++) {
      if (row.getCell(l)==null){
        // fixme at 19/05/17 getLastCellNum不一定是准确的，也有可能包含空白列
        list.add(null);
      } else {
        list.add(castToType(getCellValue(row.getCell(l)), String.class));
      }
    }
    //从尾巴开始剔除空白
    for (int i = list.size()-1; i>=0; i--){
      if (list.get(i)==null){
        list.remove(i);
      } else {
        return list;
      }
    }
    return list;
  }

  /**
   *
   * @param sheetNum 1 based
   * @param <T>
   * @return
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws NoSuchMethodException
   * @throws IOException
   */
  @SuppressWarnings("Duplicates")
  private <T>  List<T> parse(final int sheetNum) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException {
    if (sheetNum > this.sheetCount){
      throw new IndexOutOfBoundsException("sheet num overflow");
    }
    int sheetIndex = sheetNum -1;
    Class clazz;
    if ((clazz = getListValue(this.columnClass, sheetIndex, null))== null){
      throw new IllegalStateException("un given data type");
    }
    if (!List.class.isAssignableFrom(clazz)){
      if (getListValue(this.excelColumns, sheetIndex, null) == null){
        throw new IllegalArgumentException("un given properties");
      }
    }
    Sheet sheet = this.workbook.getSheetAt(sheetIndex);
    Integer titleRows = getListValue(this.skipRows, sheetIndex, 0);
    int rowLimit = getListValue(this.limitRows, sheetIndex, 0) + titleRows -1;
    if (rowLimit < titleRows){
      //=0无限制
      rowLimit = sheet.getLastRowNum();    // 0 based row num
    }else {
      rowLimit = Math.min(rowLimit, sheet.getLastRowNum());
    }
    List<T> sheetData = new LinkedList<>();

    Map<Integer, Integer> colMap = getListValue(this.columnMap, sheetIndex, Collections.emptyMap());
    List<String> sheetColumns = getListValue(this.excelColumns, sheetIndex, Collections.emptyList());
    Map<String, Method> sheetMethods = getListValue(this.classSetters, sheetIndex, Collections.emptyMap());
    int emptyRowCount = 0;
    for (int rowIndex = titleRows; rowIndex <= rowLimit; rowIndex ++){
      Row row = sheet.getRow(rowIndex);
      if (row == null){
        if ( ++emptyRowCount >= emptyRowExitThreshold){
          break;
        }
        continue;
      } else if (emptyRowCount>0){
        emptyRowCount = 0;
      }
      T rowData = (T)readRow(row, colMap, clazz, sheetColumns, sheetMethods);
      if (rowData != null) {
        sheetData.add(rowData);
      }
    }
    postHandler();
    return sheetData;
  }

  @SuppressWarnings("Duplicates")
  private<T> Stream<T> parseToStream(final int sheetNum1Based) throws NoSuchMethodException {
    preHandler();
    if (sheetNum1Based > this.sheetCount){ throw new IllegalArgumentException("sheet num overflow"); }
    int sheetIndex = sheetNum1Based -1;
    final Class<T> sheetClass = getListValue(this.columnClass, sheetIndex, null);
    final List<String> sheetColumns = getListValue(this.excelColumns, sheetIndex, Collections.emptyList());
    final Map<String, Method> sheetMethods = getListValue(this.classSetters, sheetIndex, Collections.emptyMap());
    if ( sheetClass == null){ throw new IllegalStateException("un given data type"); }
    if (!List.class.isAssignableFrom(sheetClass)){
      if (sheetColumns.size() == 0){
        throw new IllegalArgumentException("un given properties");
      }
      if (Map.class.isAssignableFrom(sheetClass)){

      } else {
        if (sheetMethods.size() == 0){
          throw new IllegalStateException("property set method not found");
        }
      }
    }
    Sheet sheet = this.workbook.getSheetAt(sheetIndex);
    int titleRows = getListValue(this.skipRows, sheetIndex, 0);
    Map<Integer, Integer> colMap = getListValue(this.columnMap, sheetIndex, Collections.emptyMap());
    Stream<T> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(sheet.rowIterator(), 0), false)
        .skip(titleRows)
        .map(i-> {
          try {
            return readRow(i,colMap, sheetClass, sheetColumns, sheetMethods );
          } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
          }
          return null;
        }).filter(Objects::nonNull);
    return stream;
  }

  private ResultSet parse() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException {
    preHandler();
    ResultSet resultSet = new ResultSet(this.sheetNames);

    if (sheetCount == 1){
      resultSet.put(1,parse(1));
      return resultSet;
    }
    if (this.sheetRead!=null && this.sheetRead.size()>0){
      for (Integer sheetNum: this.sheetRead){
        List<Object> sheetData = null;
        try {
          sheetData = parse(sheetNum);
        } catch (IndexOutOfBoundsException e) {
          sheetData = Collections.emptyList();
        }
        resultSet.put(sheetNum, sheetData);
      }
      return resultSet;
    }
    if (sheetCount>1){
      for (int i = 1; i <= this.sheetCount; i++){
        resultSet.put(i, parse(i));
      }
      return resultSet;
    }
    return resultSet;
  }

  private<T> T readRow(Row row, Map<Integer, Integer> colMap, Class<T> clazz, List<String> properties, Map<String, Method> methodMap) throws IllegalAccessException, InstantiationException, InvocationTargetException {
    T rowData = clazz.newInstance();
    boolean isEmpty = true;
    if (rowData instanceof List){
      if (colMap.size() == 0){
        for (int i = 0; i < row.getLastCellNum(); i ++){
          Object obj = getCellValue(row.getCell(i));
          ((List)rowData).add(obj);
          if (obj!=null){ isEmpty = false; }
        }
      } else {
        for (Map.Entry<Integer, Integer> entry: colMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).collect(Collectors.toList())){
          Object obj = getCellValue(row.getCell(entry.getKey()));
          ((List)rowData).add(obj);
          if (obj!=null){ isEmpty = false; }
        }
      }
      if (isEmpty){
        return null;
      }
      return rowData;
    }
    for (int x = 0; x < row.getLastCellNum(); x++){
      if (colMap.size()>0 && !colMap.keySet().contains(x)) {continue;}
      int i = colMap.size()>0?colMap.get(x):x;
      if (i>=properties.size() || properties.get(i) == null || "".equals(properties.get(i))){
        continue;
      }
      Cell cell = row.getCell(x);
      Object val = getCellValue(cell);
      if (val == null) {continue;}
      if (rowData instanceof Map){
        ((Map) rowData).put(properties.get(i), val);
      } else {
        Method method = methodMap.get(properties.get(i));
        Class paramType = method.getParameterTypes()[0];
        Object obj = null;
        try {
          obj = castToType(val, paramType);
          method.invoke(rowData, paramType.cast(obj));
        } catch (Exception e) {
          continue;
        }
      }
      isEmpty = false;
    }
    if (isEmpty){
      return null;
    }
    return rowData;
  }

  private Object getCellValue(Cell cell){
    if (cell == null) {return null;}
    switch (cell.getCellType()){
      case STRING:
        return cell.getStringCellValue();
      case NUMERIC:
        if (DateUtil.isCellDateFormatted(cell)){
          return DateUtil.getJavaDate(cell.getNumericCellValue());
        } else {
          return cell.getNumericCellValue();
        }
      case BOOLEAN:
        return cell.getBooleanCellValue();
      case _NONE:
      case BLANK:
      case ERROR:
        return null;
      case FORMULA:
        //CellType type = cell.getCachedFormulaResultType();
        //val = cell.getRichStringCellValue().toString();
        if (this.formulaEvaluator == null){
          this.formulaEvaluator = isXlsx?new XSSFFormulaEvaluator((XSSFWorkbook) this.workbook):new HSSFFormulaEvaluator((HSSFWorkbook) this.workbook);
        }
        CellValue value = formulaEvaluator.evaluate(cell);
        switch (value.getCellType()){
          case STRING:
            return value.getStringValue();
          case NUMERIC:
            return value.getNumberValue();
          case BOOLEAN:
            return value.getBooleanValue();
          case BLANK:
          case ERROR:
          case _NONE:
          default:
            return null;
        }
      default:
        return null;
    }
  }

  private Date parseDate(String date){
    Calendar.Builder builder = new Calendar.Builder();
    Matcher matcher = datetimePattern.matcher(date);
    if (matcher.matches()){
      return builder
          .setDate(Integer.valueOf(matcher.group("year")), Integer.valueOf( matcher.group("mon")),Integer.valueOf(matcher.group("day")))
          .setTimeOfDay(Integer.valueOf(matcher.group("hour")), Integer.valueOf(matcher.group("min")), Integer.valueOf(matcher.group("sec")))
          .build().getTime();
    }
    matcher = datePattern.matcher(date);
    if (matcher.matches()){
      return builder.setDate(Integer.valueOf(matcher.group("year")), Integer.valueOf( matcher.group("mon")),Integer.valueOf(matcher.group("day"))).build().getTime();
    }
    matcher = timePattern.matcher(date);
    if (matcher.matches()){
      return builder.setTimeOfDay(Integer.valueOf(matcher.group("hour")), Integer.valueOf(matcher.group("min")), Integer.valueOf(matcher.group("sec"))).build().getTime();
    }
    throw new ClassCastException(date + "cannot be cast to Date");
  }

  private<T> T castToType(Object obj,  Class<T> type){
    if (obj == null) return null;
    if (obj instanceof String){
      if (String.class.isAssignableFrom(type)){
        //fixme @20100104 By sqni 取消String类型的trim, trim之后可能会和原先的数据不一致，导致diff比较的时候产生不一致
        return (T) obj;
      } else if (Integer.class.isAssignableFrom(type)){
        return (T) Integer.valueOf((String) obj);
      } else if (Double.class.isAssignableFrom(type)) {
        return (T) Double.valueOf((String) obj);
      } else if (Boolean.class.isAssignableFrom(type)){
        boolean t = "true".equalsIgnoreCase(((String) obj).trim())
            || "1".equals(((String) obj).trim())
            || "yes".equalsIgnoreCase(((String) obj).trim())
            || "T".equalsIgnoreCase(((String) obj).trim())
            || "是".equals(((String) obj).trim());
        if (t){return (T) Boolean.valueOf(true);}
        boolean f = "false".equalsIgnoreCase(((String) obj).trim())
            || "0".equals(((String) obj).trim())
            || "no".equalsIgnoreCase(((String) obj).trim())
            || "F".equalsIgnoreCase(((String) obj).trim())
            || "否".equals(((String) obj).trim());
        if (f){ return (T) Boolean.valueOf(false);}
        return null;
      } else if (Date.class.isAssignableFrom(type)){
        return (T) parseDate((String) obj);
      }
    } else if (obj instanceof Double){
      if (Double.class.isAssignableFrom(type)){
        return (T) obj;
      }
      if (String.class.isAssignableFrom(type)){
        if ((Double) obj -((Double) obj).longValue()==0){
          return (T) String.valueOf(((Double) obj).longValue());
        }
        return (T) obj.toString();
      } else if (Integer.class.isAssignableFrom(type)){
        return (T) (Integer)((Double) obj).intValue();
      } else if (Date.class.isAssignableFrom(type)){
        return (T) DateUtil.getJavaDate((Double) obj);
      } else if (Boolean.class.isAssignableFrom(type)){
        return (T) Boolean.valueOf(!obj.equals(0));
      }
    } else if (obj instanceof Boolean){
      if (String.class.isAssignableFrom(type)){
        return (T) ((Boolean)obj?"true":"false");
      } else if (Integer.class.isAssignableFrom(type)){
        return (T) Integer.valueOf ((Boolean)obj?1:0);
      } else if (Double.class.isAssignableFrom(type)){
        return (T) Double.valueOf ((Boolean)obj?1.0:0.0);
      } else if (Date.class.isAssignableFrom(type)){
        throw new ClassCastException("Cannot cast Boolean to Date");
      }
    } else if (obj instanceof Date){
      if (String.class.isAssignableFrom(type)){
        return (T) String.format("%tc", obj);
      } else if (Integer.class.isAssignableFrom(type)){
        return (T) Integer.valueOf ((int)((Date)obj).getTime());
      } else if (Double.class.isAssignableFrom(type)){
        return (T) (Double.valueOf((double)((Date)obj).getTime()));
      } else if (Date.class.isAssignableFrom(type)){
        return (T) obj;
      }
    }
    return type.cast(obj);
  }

  private void postHandler() throws IOException {
    if (this.opcPackage!=null){
      this.opcPackage.close();
    }
    if (this.poifsFileSystem!=null){
      this.poifsFileSystem.close();
    }
  }

  private <E> List<E> setListValue(List<E> list, int index,E data, E defaultValue, int initialCapacity){
    if (list == null){
      list = new ArrayList<>(initialCapacity);
    }
    if (index<0){
      list.add(data);
    } else if (list.size()>index){
      list.set(index, data);
    } else {
      for (int i = list.size(); i<index; i++){
        list.add(defaultValue);
      }
      list.add(data);
    }
    return list;
  }

  private <E> E getListValue(List<E> list, int index, E defaultValue){
    if (list == null){
      return defaultValue;
    }
    if (index<0 || index>=list.size()){
      return defaultValue;
    }
    E t = list.get(index);
    if (t == null){
      return defaultValue;
    }
    return t;
  }

  private String fileExtension(String fileName){
    if (fileName==null){
      return null;
    }
    int intExtension = fileName.lastIndexOf('.');
    if (intExtension<0){
      return "";
    }
    return fileName.substring(intExtension+1);
  }

  public static class ResultSet {
    private Map<Integer, List<Object>> resultMapBySheetIndex = new HashMap<>();
    private Map<String, Integer> sheetNameMap = new HashMap<>();

    protected ResultSet(Map<String, Integer> sheetNameMap){
      this.sheetNameMap = sheetNameMap;
    }
    protected ResultSet(List<String> sheetNames){
      for (int i = 0; i < sheetNames.size(); i++){
        sheetNameMap.put(sheetNames.get(i), i+1);
      }
    }

    protected void put(Integer sheetIndex, List<Object> list){
      resultMapBySheetIndex.put(sheetIndex, list);
    }

    protected void put(String sheetName, List<Object> list){
      resultMapBySheetIndex.put(sheetNameMap.get(sheetName), list);
    }

    public<T> List<T> get(Integer sheetIndex){
      return (List<T>) resultMapBySheetIndex.getOrDefault(sheetIndex, Collections.emptyList());
    }

    public<T> List<T> get(String sheetName){
      return (List<T>) resultMapBySheetIndex.getOrDefault(sheetNameMap.get(sheetName), Collections.emptyList());
    }

    public<T> List<T> find(String regex){
      List<Integer> sheetIndexes = new ArrayList<>();
      for (String key: sheetNameMap.keySet()){
        if (key.matches(regex)){
          sheetIndexes.add(sheetNameMap.get(key));
        }
      }
      if (sheetIndexes.size() == 0){
        return Collections.emptyList();
      } else if (sheetIndexes.size()>1) {
        sheetIndexes.removeIf(index -> resultMapBySheetIndex.get(index) == null);
        if (sheetIndexes.size()>1) {
          throw new IllegalStateException("dual sheet matches");
        }
      }
      return (List<T>) resultMapBySheetIndex.getOrDefault(sheetIndexes.get(0), Collections.emptyList());
    }

    public<T> List<List<T>> values(){
      return resultMapBySheetIndex.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(Map.Entry::getValue).map(l->(List<T>)l).collect(Collectors.toList());
    }

    public int size(){
      return resultMapBySheetIndex.size();
    }
  }

  private static abstract class BeanUtil {

    public static List<Method> getBeanSetters(Class<?> clazz) {
      Class cl = clazz;
      List<Method> methodList = new LinkedList<>();
      while (cl != null && !cl.equals(Object.class)) {
        Method[] methods = cl.getDeclaredMethods();
        for (Method m : methods) {
          if (m.getParameterCount() == 0 || m.getParameterCount() > 1){
            continue;
          }
          if (Modifier.isProtected(m.getModifiers())
                  || Modifier.isAbstract(m.getModifiers())
                  ||Modifier.isPrivate(m.getModifiers())){
            continue;
          }
          if (m.getName().startsWith("set")) {
            methodList.add(m);
          } /*else if (m.getName().startsWith("is") && m.getReturnType().equals(Boolean.class)){
            methodList.add(m);
          }*/
        }
        cl = cl.getSuperclass();
      }
      return methodList;
    }

    public static Method getBeanSetter(Class<?> clazz, String fieldName, Class<?>... parameterTypes) throws NoSuchMethodException {
      Class<?> cl = clazz;
      String field = capture(fieldName);
      while (cl != null) {
        try {
          return cl.getDeclaredMethod("set" + field, parameterTypes);
        } catch (NoSuchMethodException var5) {
          cl = cl.getSuperclass();
        }
      }
      throw new NoSuchMethodException(fieldName);
    }

    public static String capture(String str) {
      char[] chars = str.toCharArray();
      if (chars.length == 0) {
        return str;   //同一对象
      } else if (chars[0] >= 'a' && chars[0] <= 'z') {
        chars[0] -= 32;
        return new String(chars);   //新对象
      } else {
        return str;   //同一对象
      }
    }

    public static String unCapture(String str) {
      char[] chars = str.toCharArray();
      if (chars.length == 0) {
        return str;   //同一对象
      } else if (chars[0] >= 'A' && chars[0] <= 'Z') {
        chars[0] += 32;
        return new String(chars);   //新对象
      } else {
        return str;   //同一对象
      }
    }

  }

  @Override
  protected void finalize() throws Throwable {
    postHandler();
    super.finalize();
  }
}
