package dev.suntr.excel;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellRangeUtil;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 导出Excel
 * 支持多个sheet
 * 支持复杂表头
 * 复杂表头可以是 List<List<String>> title。会自动按行优先对相同内容进行合并
 * 例如
 *        {'a','b','c','d'},
 *        {'a','b','c'},
 *        {'a','c','d','e'},
 *        {'b','c','e','e'}
 * 会自动合并生成
 *        ┏┉┉┉┉┉┉┉┉┉┉┉┯┉┉┉┓
 *        ┃     a     │ b ┃
 *        ┠┈┈┈┈┈┈┈┬┈┈┈┴┈┈┈┨
 *        ┃   b   │   c   ┃
 *        ┠┈┈┈┬┈┈┈┼┈┈┈┬┈┈┈┩
 *        ┃ c │   │ d │ e ┃
 *        ┠┈┈┈┤ c ├┈┈┈┴┈┈┈┨
 *        ┃ d │   │   e   ┃
 *        ┗┉┉┉┷┉┉┉┷┉┉┉┉┉┉┉┛
 * 支持List, Map 和 任意bean数据类型。
 *
 * @author suntrian
 * @date 2019.1.04
 * @since 1.5.9
 */
@SuppressWarnings("all")
public final class ExcelWriter {

  public enum BorderPosition{
    TOP, BOTTOM, LEFT, RIGHT, ALL
  }

  protected Workbook workbook;

  private int sheetCount = 1;

  private List<Class> type;

  private CellStyle titleStyle;

  private CellStyle bodyStyle;

  private List<Map<CellRangeAddress, CellStyle>> customStyle;

  //自动设置列宽
  private List<Boolean> autoCellWidth;

  //顺序输出属性
  private List<List<String>> sortedFields;

  private List<Map<String,Method>> beanGetMethods;

  private List<String> sheetNames;

  private List<List<?>> titles;

  //复杂表头的合并策略，true:水平合并优先,false:垂直合并优先
  private List<Boolean> titleMergeHorizonFirst;

  private List<List<Integer>> columnWidths;

  private List<Integer> defaultWidths;
  private Integer defaultWidth;

  private List<Map<Integer, List<String>>> cellConstraint;

  private List<List<?>> data;

  private Stream<?> dataSteam;

  private static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss";
  private SimpleDateFormat dateFormat = new SimpleDateFormat(datetimeFormat);
  private DataFormat dataFormat;

  private Map<String, CellStyle> fontStyleCache = new ConcurrentHashMap<>();
  private Map<String, CellStyle> cellColorStyleCache = new ConcurrentHashMap<>();
  private Map<CellData, CellStyle> cellDataStyleCache = new ConcurrentHashMap<>();
  private List<Pair<List<? extends Pair<Integer, Integer>>,List<? extends Pair<Integer, Integer>>>> customSytleRanges = new ArrayList<>(this.sheetCount);

  private Workbook newWorkbook(){
    //return new XSSFWorkbook();
    return new SXSSFWorkbook(); // 无法读，导致无法在postHandler里面设置单元格属性
  }

  public ExcelWriter setSheetCount(int size){
    this.sheetCount = size;
    return this;
  }

  public ExcelWriter setDataType(Class clazz){
    this.type = setList(this.type, clazz, sheetCount);
    return this;
  }

  /**
   * 设置每个sheet的数据类型，不设置时将根据传入的数据data自动推断
   * @param clazz
   * @param sheetNum based from 1
   * @return
   */
  public ExcelWriter setDataType(int sheetNum, Class clazz){
    this.type = setList(this.type, sheetNum-1, clazz, null, sheetCount);
    return this;
  }

  public ExcelWriter setAutoWidth(boolean autoWidth){
    this.autoCellWidth = setList(this.autoCellWidth, autoWidth, sheetCount);
    return this;
  }

  public ExcelWriter setAutoWidth(int sheetNum, boolean autoWidth){
    this.autoCellWidth = setList(this.autoCellWidth, sheetNum-1, autoWidth, false, sheetCount);
    return this;
  }

  public ExcelWriter setWidth(int columnIndex, int width){
    this.columnWidths = setList(this.columnWidths, setList(getList(this.columnWidths, -1, new ArrayList<>()), columnIndex, width, null), sheetCount);
    return this;
  }

  public ExcelWriter setWidth(int sheetNum, int columnIndex, int width){
    this.columnWidths = setList(this.columnWidths, sheetNum-1, setList(getList(this.columnWidths, sheetNum-1, new ArrayList<>()), columnIndex, width, null), Collections.emptyList());
    return this;
  }

  public ExcelWriter setDefaultWidth(int sheetNum, int width){
    this.defaultWidths = setList(this.defaultWidths, sheetNum-1, width, null, sheetCount);
    return this;
  }

  public ExcelWriter setDefaultWidth(int width){
    this.defaultWidth = width;
    return this;
  }

  private ExcelWriter setBodyCellStyle(){
    if (this.workbook==null){
      this.workbook = newWorkbook();
    }
    if (this.bodyStyle == null) {
      this.titleStyle = workbook.createCellStyle();
    }
    return this;
  }

  public ExcelWriter setBodyCellStyleFont(String fontName, Integer fontSize, Integer fontColor){
    setBodyCellStyle();
    this.bodyStyle = setCellStyleFont(this.bodyStyle, fontName, fontSize, fontColor);
    return this;
  }

  public ExcelWriter setBodyCellStyleAlign(HorizontalAlignment horizontalAlignment, VerticalAlignment verticalAlignment){
    setBodyCellStyle();
    this.bodyStyle = setCellStyleAlign(this.bodyStyle, horizontalAlignment, verticalAlignment);
    return this;
  }

  public ExcelWriter setBodyCellStyleBorder(BorderStyle borderStyle, Integer color, BorderPosition... positions) {
    setBodyCellStyle();
    this.bodyStyle = setCellStyleBorder(this.bodyStyle, borderStyle, color, positions);
    return this;
  }

  private ExcelWriter setTitleCellStyle(){
    if (this.workbook==null){
      this.workbook = newWorkbook();
    }
    if (this.titleStyle == null){
      this.titleStyle = workbook.createCellStyle();
      this.titleStyle.setAlignment(HorizontalAlignment.CENTER);
      this.titleStyle.setVerticalAlignment(VerticalAlignment.CENTER);
      Font font = this.workbook.createFont();
      font.setFontHeightInPoints((short) 12);
      font.setFontName("黑体");
      this.titleStyle.setFont(font);
    }
    return this;
  }

  public ExcelWriter setTitleCellStyleFont(String fontName, Integer fontSize, Integer fontColor){
    setTitleCellStyle();
    this.titleStyle = setCellStyleFont(this.titleStyle, fontName, fontSize, fontColor);
    return this;
  }

  public ExcelWriter setTitleCellStyleAlign(HorizontalAlignment horizontalAlignment, VerticalAlignment verticalAlignment){
    setTitleCellStyle();
    this.titleStyle = setCellStyleAlign(this.titleStyle, horizontalAlignment, verticalAlignment);
    return this;
  }

  public ExcelWriter setTitleCellStyleBorder(BorderStyle borderStyle, Integer color, BorderPosition... positions) {
    setTitleCellStyle();
    this.titleStyle = setCellStyleBorder(this.titleStyle, borderStyle, color, positions);
    return this;
  }

  private CellStyle setCellStyleFont(CellStyle style,
                                     String fontName,
                                     Integer fontSize,
                                     Integer fontColor,
                                     Boolean italic,
                                     Boolean bold,
                                     Boolean strikeOut,
                                     Byte underLine){
    if (fontName==null&&fontSize==null){
      return style;
    }
    Font font = this.workbook.createFont();
    if (fontSize!=null) {font.setFontHeightInPoints(fontSize.shortValue());}
    if (fontName!=null) {font.setFontName(fontName);}
    if (fontColor!=null) {font.setColor(fontColor.shortValue());}
    if ( italic != null) {font.setItalic(italic);}
    if ( bold != null) {font.setBold(bold);}
    if ( strikeOut != null) {font.setStrikeout(strikeOut);}
    if ( underLine != null) {font.setUnderline(underLine);}
    style.setFont(font);
    return style;
  }

  private CellStyle setCellStyleFont(CellStyle style,
                                     String fontName,
                                     Integer fontSize,
                                     Integer fontColor){
    return setCellStyleFont(style, fontName, fontSize, fontColor, null, null, null, null);
  }

  private CellStyle setCellStyleAlign(CellStyle style, HorizontalAlignment horizontalAlignment, VerticalAlignment verticalAlignment){
    if (horizontalAlignment!=null){ style.setAlignment(horizontalAlignment); }
    if (verticalAlignment!=null){ style.setVerticalAlignment(verticalAlignment); }
    return style;
  }

  private CellStyle setCellStyleBorder(CellStyle style, BorderStyle borderStyle, Integer color, BorderPosition... positions){
    boolean all = false;
    if (positions.length==0 || Arrays.asList(positions).contains(BorderPosition.ALL)){
      all = true;
      positions = new BorderPosition[]{BorderPosition.ALL};
    }
    for (BorderPosition position: positions){
      switch (position){
        case ALL:
          all = true;
        case TOP:
          if (borderStyle!=null) {style.setBorderTop(borderStyle);};
          if (color!=null)       {style.setTopBorderColor(color.shortValue()) ;};
          if (!all) break;
        case BOTTOM:
          if (borderStyle!=null) {style.setBorderBottom(borderStyle);}
          if (color!=null)       {style.setBottomBorderColor(color.shortValue());}
          if (!all) break;
        case LEFT:
          if (borderStyle!=null) {style.setBorderLeft(borderStyle);};
          if (color!=null)       {style.setLeftBorderColor(color.shortValue());};
          if (!all) break;
        case RIGHT:
          if (borderStyle!=null) {style.setBorderRight(borderStyle);};
          if (color!=null)       {style.setRightBorderColor(color.shortValue());};
          if (!all) break;
      }
    }
    return style;
  }

  private CellStyle setCellStyleColor(CellStyle style, Integer backgroundColor, Integer foregroundColor) {
    if (backgroundColor!=null){style.setFillBackgroundColor(backgroundColor.shortValue());}
    if (foregroundColor!=null){
      style.setFillForegroundColor(foregroundColor.shortValue());
      style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
    }
    return style;
  }
  /**
   * 默认设置第一个sheet的style
   * @param fontName
   * @param fontSize
   * @param startRow
   * @param endRow
   * @param startCol
   * @param endCol
   * @return
   */
  public ExcelWriter setCustomCellStyleFont(String fontName, Integer fontSize, Integer fontColor, int startRow, int endRow, int startCol, int endCol){
    setCustomCellStyleFont(1, fontName, fontSize, fontColor, startRow, endRow, startCol, endCol);
    return this;
  }

  public ExcelWriter setCustomCellStyleFont(int sheetNumBased1, String fontName, Integer fontSize, Integer fontColor, int startRow, int endRow, int startCol, int endCol){
    CellRangeAddress addresses = new CellRangeAddress(startRow, endRow, startCol, endCol);
    CellStyle style;
    Map<CellRangeAddress, CellStyle> styleMap ;
    if ((styleMap = getList(this.customStyle, sheetNumBased1-1, new HashMap<>())).containsKey(addresses)){
      style = styleMap.get(addresses);
      setCellStyleFont(style, fontName, fontSize, fontColor);
    } else {
      String cacheKey = fontName+fontSize+fontColor;
      if (fontStyleCache.containsKey(cacheKey)){
        style = fontStyleCache.get(cacheKey);
      } else {
        if (this.workbook == null){ this.workbook = newWorkbook(); }
        style = this.workbook.createCellStyle();
        setCellStyleFont(style, fontName, fontSize, fontColor);
        fontStyleCache.put(cacheKey, style);
      }
    }
    styleMap.put(addresses, style);
    this.customStyle = setList(this.customStyle, sheetNumBased1-1, styleMap, new HashMap<>(1), sheetCount);
    return this;
  }

  public ExcelWriter setCustomCellStyleColor(int sheetNum, Integer backgroundColor, Integer foregroundColor, int startRow, int endRow, int startCol, int endCol){
    CellRangeAddress addresses = new CellRangeAddress(startRow, endRow, startCol, endCol);
    CellStyle style;
    Map<CellRangeAddress, CellStyle> styleMap ;
    if ((styleMap = getList(this.customStyle, sheetNum-1, new HashMap<>())).containsKey(addresses)){
      style = styleMap.get(addresses);
      setCellStyleColor(style, backgroundColor, foregroundColor);
    } else {
      String cacheKey = String.format("%d:%d", backgroundColor, foregroundColor);
      if (cellColorStyleCache.containsKey(cacheKey)){
        style = cellColorStyleCache.get(cacheKey);
      } else {
        if (this.workbook == null){ this.workbook = newWorkbook(); }
        style = this.workbook.createCellStyle();
        setCellStyleColor(style, backgroundColor, foregroundColor);
        cellColorStyleCache.put(cacheKey, style);
      }
    }
    styleMap.put(addresses, style);
    this.customStyle = setList(this.customStyle, sheetNum-1, styleMap, new HashMap<>(1),sheetCount);
    return this;
  }

  public ExcelWriter setCustomCellStyleColor(Integer backgroundColor, Integer foregroundColor, int startRow, int endRow, int startCol, int endCol){
    return setCustomCellStyleColor(1, backgroundColor, foregroundColor, startRow, endRow, startCol, endCol);
  }

  public ExcelWriter setSortedFields(List<String> sortedFields){
    this.sortedFields = setList(this.sortedFields, sortedFields, sheetCount);
    return this;
  }

  public ExcelWriter setSortedFields(int sheetNum, List<String> sortedFields){
    this.sortedFields = setList(this.sortedFields,sheetNum-1, sortedFields, null, sheetCount);
    return this;
  }

  public ExcelWriter setSortedFields(String... sortedFields){
    this.sortedFields = setList(this.sortedFields, Arrays.asList(sortedFields), sheetCount);
    return this;
  }

  public ExcelWriter setSheetNames(String sheetNames) {
    this.sheetNames = setList(this.sheetNames, sheetNames, sheetCount);
    return this;
  }

  public ExcelWriter setSheetNames(int sheetNum, String sheetName){
    this.sheetNames = setList(this.sheetNames, sheetNum-1, sheetName, null, sheetCount);
    this.sheetCount = sheetNum>this.sheetCount?sheetNum:sheetCount;
    return this;
  }

  public ExcelWriter setTitles(List<?> titles){
    this.titles = setList(this.titles, titles, sheetCount);
    return this;
  }

  public ExcelWriter setTitles(int sheetNum, List<?> titles){
    this.titles = setList(this.titles, sheetNum-1, titles, Collections.emptyList(), sheetCount);
    this.sheetCount = sheetNum>this.sheetCount?sheetNum:sheetCount;
    return this;
  }

  public ExcelWriter setTitles(String... titles)  {
    this.titles = setList(this.titles, Arrays.asList(titles), sheetCount);
    return this;
  }

  public ExcelWriter setTitleMergeHorizonFirst(Boolean horizonFirst){
    this.titleMergeHorizonFirst = setList(this.titleMergeHorizonFirst, horizonFirst, sheetCount);
    return this;
  }

  public ExcelWriter setTitleMergeHorizonFirst(int sheetNum, Boolean horizonFirst){
    this.titleMergeHorizonFirst = setList(this.titleMergeHorizonFirst, sheetNum-1, horizonFirst, true, sheetCount);
    return this;
  }

  /**
   * 设置单元格下拉选项
   * @param sheetNum   第几个sheet,based 1
   * @param columnNum  第几列 based 1
   * @param constaint
   * @return
   */
  public ExcelWriter setCellConstraint(int sheetNum, int columnNum, List<String> constaint){
    if (this.cellConstraint!=null && this.cellConstraint.size()>=sheetNum){
      Map<Integer, List<String>> constainMap = this.cellConstraint.get(sheetNum-1);
      if (constainMap!=null){
        constainMap.put(columnNum, constaint);
      } else {
        constainMap = new HashMap<>();
        constainMap.put(columnNum, constaint);
        this.cellConstraint.set(sheetNum-1, constainMap);
      }
    } else {
      Map<Integer, List<String>> constaintMap = new HashMap<>();
      constaintMap.put(columnNum, constaint);
      this.cellConstraint = setList(this.cellConstraint, sheetNum-1, constaintMap, new HashMap<>(), sheetCount);
    }
    return this;
  }

  public ExcelWriter setCellConstraint(int columnNum, List<String> constaint) {
    return setCellConstraint(this.cellConstraint==null?1:this.cellConstraint.size()+1, columnNum, constaint);
  }

  public ExcelWriter setData(List<?> data){
    this.data = setList(this.data, data, sheetCount);
    this.dataSteam = null;
    return this;
  }

  public ExcelWriter setData(int sheetNum, List<?> data){
    this.data = setList(this.data, sheetNum-1, data, Collections.emptyList(), sheetCount);
    this.dataSteam = null;
    this.sheetCount = sheetNum>this.sheetCount?sheetNum:sheetCount;
    return this;
  }



  public ExcelWriter setData(Stream<?> dataSteam){
    this.dataSteam = dataSteam;
    this.data = null;
    return this;
  }

  @SuppressWarnings("unchecked")
  private void preHandler() throws NoSuchMethodException {
    //检测是否存在数据，以数据或者标题的数量来判定sheet数量
    if (this.data==null || this.data.size()==0){
      if (this.titles==null || this.titles .size() == 0){
        throw new IllegalArgumentException("未设置数据");
      } else {
        this.sheetCount = this.titles.size()>this.sheetCount?this.titles.size():this.sheetCount;
      }
      this.data = new ArrayList<>(sheetCount);
    } else {
      this.sheetCount = this.data.size()>this.sheetCount?this.data.size():this.sheetCount;
    }

    //判断输入的数据类型, 并根据数据类型生成fields, 如果是bean类型则缓存getter方法
    for (int i = 0 ; i < sheetCount; i ++){
      if (getList(this.data,i, Collections.EMPTY_LIST).size()>0){
        //判断数据类型
        Class clazz = null;
        if ((clazz = getList(this.type,i,null)) == null){
          if (getList(this.data.get(i), 0, null) == null){
            continue;
          }
          clazz = this.data.get(i).get(0).getClass();
        }
        if (List.class.isAssignableFrom(clazz)){

        } else if (Map.class.isAssignableFrom(clazz)){
          if (getList(this.sortedFields, i, Collections.EMPTY_LIST).size() == 0){
            if (getList(this.data,i,Collections.EMPTY_LIST).size()>0){
              List<String> keys = new ArrayList<> (((Map)this.data.get(i).get(0)).keySet());
              this.sortedFields = setList(this.sortedFields, i, keys, Collections.EMPTY_LIST, sheetCount);
            } else {

            }
          }
        } else {
          if (getList(this.sortedFields,i, Collections.EMPTY_LIST).size() > 0){
            //指定了输出属性及顺序，则按指定的顺序输出
            Map<String, Method> methodMap = new HashMap<>(this.sortedFields.get(i).size());
            for (String field: this.sortedFields.get(i)){
              if (field == null || "".equals(field.trim())){
                continue;
              }
              methodMap.put(field, BeanUtil.getBeanGetter(clazz, field));
            }
            this.beanGetMethods = setList(this.beanGetMethods, i, methodMap, Collections.EMPTY_MAP, sheetCount);
          } else {
            //没有指定输出属性，则获取所有属性输出
            LinkedList<Method> getters = (LinkedList<Method>) BeanUtil.getBeanGetters(clazz);
            List<String> fields = new ArrayList<>(getters.size());
            Map<String, Method> methodMap = new HashMap<>(getters.size());
            Method getter;
            while ((getter = getters.pollLast())!=null){
              //从尾开始遍历，子类同名方法覆盖父类方法
              String fieldName = BeanUtil.unCapture(getter.getName().substring(3));
              fields.add(fieldName);
              methodMap.put(fieldName, getter);
            }
            this.sortedFields = setList(this.sortedFields, i, fields, Collections.EMPTY_LIST, sheetCount);
            this.beanGetMethods = setList(this.beanGetMethods, i, methodMap, Collections.EMPTY_MAP, sheetCount);
          }
        }
        this.type = setList(this.type, i, clazz, null, sheetCount);
      }

    }
    setTitleCellStyle();

    if (this.customStyle!=null && !this.customStyle.isEmpty()) {
      //处理CustomStyle，提高性能，不用每次都在所有CellRangeAddress里面判断cell是否有自定义Style
      Method mergeCellRange = BeanUtil.findMethod(CellRangeUtil.class, "mergeCellRanges", List.class);
      //坑爹的POI里面的这个方法是个private方法， 使用反射虽然效率效低，但是会比调用Array的重载方法要先 toArray再转List会好一些。
      mergeCellRange.setAccessible(true);
      try {
        for (ListIterator<Map<CellRangeAddress, CellStyle>> iterator = this.customStyle.listIterator(); iterator.hasNext(); ) {
          Map<CellRangeAddress, CellStyle> cellStyleMap = iterator.next();
          if (cellStyleMap == null || cellStyleMap.isEmpty()) {
            setList(this.customSytleRanges, null, this.sheetCount);
            continue;
          }
          Map<CellStyle, List<CellRangeAddress>> cellRangeByStyle = cellStyleMap.<CellRangeAddress, CellStyle>entrySet().stream()
                  .collect(Collectors.groupingBy(Map.Entry::getValue, LinkedHashMap::new, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
          for (Map.Entry<CellStyle, List<CellRangeAddress>> cellStyleListEntry : cellRangeByStyle.entrySet()) {
            cellStyleListEntry.setValue((List<CellRangeAddress>) mergeCellRange.invoke(null, cellStyleListEntry.getValue()));
            //cellStyleListEntry.setValue(Arrays.asList(CellRangeUtil.mergeCellRanges(cellStyleListEntry.getValue().toArray(new CellRangeAddress[0]))));
          }
          cellStyleMap = new LinkedHashMap<>();
          for (Map.Entry<CellStyle, List<CellRangeAddress>> entry : cellRangeByStyle.entrySet()) {
            for (CellRangeAddress cellAddresses : entry.getValue()) {
              cellStyleMap.put(cellAddresses, entry.getKey());
            }
          }
          iterator.set(cellStyleMap);

          //处理横竖坐标，生成类似于布隆过滤器快速过滤无style的cell
          List<Pair<Integer, Integer>> xRangeList = cellStyleMap.keySet().stream().map(x-> Pair.of(x.getFirstColumn(), x.getLastColumn())).sorted(Comparator.comparing(Pair::getLeft)).collect(Collectors.toList());
          handleRangeList(xRangeList);

          List<Pair<Integer, Integer>> yRangeList = cellStyleMap.keySet().stream().map(x-> Pair.of(x.getFirstRow(), x.getLastRow())).sorted(Comparator.comparing(Pair::getLeft)).collect(Collectors.toList());
          handleRangeList(yRangeList);
          setList(this.customSytleRanges, Pair.of(xRangeList, yRangeList), this.sheetCount);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        //不会发生这两个错误的。除非POI版本改变导致反射方法失效
      }
    }
  }

  private void handleRangeList(List<Pair<Integer, Integer>> rangeList) {
    Pair<Integer, Integer> formerOne = Pair.of(Integer.MIN_VALUE, Integer.MIN_VALUE);
    for (Iterator<Pair<Integer, Integer>> iter = rangeList.iterator(); iter.hasNext(); ) {
      Pair<Integer, Integer> pair = iter.next();
      if (pair.getLeft() > formerOne.getRight() + 1) {
        formerOne = pair;
        continue;
      } else {
        if (pair.getRight() > formerOne.getRight()) {
          formerOne.setRight(pair.getRight());
        }
        iter.remove();
      }
    }
  }

  public Workbook generate() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    if (this.dataSteam == null){
      return generate(sheetNames, titles, sortedFields, data);
    }else if (this.dataSteam != null){
      return generate(getList(this.sheetNames,0,null), getList(this.titles, 0, Collections.EMPTY_LIST), getList(this.sortedFields, 0, Collections.EMPTY_LIST), dataSteam);
    } else {
      throw new IllegalArgumentException("未提供数据");
    }
  }

  public Workbook generate(final String sheetName,final List<?> titles, final List<String> sortedFields, final List<?> datas) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    return generate(
            Collections.singletonList(sheetName),
            Collections.singletonList(titles),
            Collections.singletonList(sortedFields),
            Collections.singletonList(datas)
            );
  }

  /**
   *
   * @param titles
   * @param data
   * @return
   */
  public Workbook generate(final List<String> sheetNames,final List<List<?>> titles,final List<List<String>> sortedFields,final List<List<?>> data) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    this.sheetNames = sheetNames;
    this.titles = titles;
    this.sortedFields = sortedFields;
    this.data = data;

    if (this.workbook == null){
      this.workbook = newWorkbook();  //new SXSSFWorkbook(); // 无法读，导致无法在postHandler里面设置单元格属性
    }
    preHandler();

    for (int sheetIndex = 0; sheetIndex < sheetCount; sheetIndex++){
      String sheetName = getList(this.sheetNames, sheetIndex, null);
      Sheet sheet;
      if (sheetName == null){
        sheet = workbook.createSheet("Sheet"+(sheetIndex+1));
      } else {
        sheet = workbook.createSheet(sheetName);
      }

      int titleRows = 0;
      List<?> title ;
      if ((title = getList(this.titles, sheetIndex, Collections.emptyList())).size()>0){
        titleRows = exportTitle(sheet, title, sheetIndex, getList(this.titleMergeHorizonFirst, sheetIndex, true));
      }
      if (this.data.size()==0){
        continue;
      }

      List<?> sheetData = getList(this.data, sheetIndex, Collections.EMPTY_LIST);
      for (int rowIndex = titleRows; rowIndex < sheetData.size() + titleRows; rowIndex++){
        Row row = sheet.createRow(rowIndex);
        Object rowData = sheetData.get(rowIndex-titleRows);
        List<Object> rowDataList = flatData(rowData, getList(this.type, sheetIndex, List.class), getList(this.sortedFields, sheetIndex, Collections.EMPTY_LIST), getList(this.beanGetMethods, sheetIndex, Collections.EMPTY_MAP));
        for (int cellIndex = 0 ; cellIndex < rowDataList.size(); cellIndex++){
          Cell cell = row.createCell(cellIndex);
          setCell(cell, rowDataList.get(cellIndex), getCellStyle(sheetIndex, cell, this.bodyStyle));
        }
      }
      postHandler(sheetIndex, sheet, titleRows, sheetData.size());

    }
    return workbook;
  }

  /**
   * 只能支持单个sheet的导出
   * 经测试直接输出到输出流失败，所以还是改成先生成workbook, 但是可以节省点内存
   * @param sheetNames
   * @param titles
   * @param sortedFields
   * @param data
   * @param <T>
   * @throws NoSuchMethodException
   */
  private<T> Workbook generate(final String sheetNames,final List<?> titles,final List<String> sortedFields, Stream<T> data) throws NoSuchMethodException {
    if (this.workbook == null){
      this.workbook = newWorkbook();
    }
    preHandler();
    Sheet sheet;
    if (sheetNames == null){
      sheet = workbook.createSheet("Sheet1");
    } else {
      sheet = workbook.createSheet(sheetNames);
    }

    int titleRows = 0;
    if (titles.size()>0){
      titleRows = exportTitle(sheet, titles, 0, true);
    }
    int[] dataRows = new int[1];
    final Map<String, Method> methodMap = getList(this.beanGetMethods, 0, Collections.EMPTY_MAP);
    data.map(i-> {
      try {
        return flatData(i,i.getClass(), sortedFields, methodMap);
      } catch (Exception e) {
        return null;
      }
    }).forEach(l->{
      dataRows[0] += 1;
      Row row = sheet.createRow(sheet.getLastRowNum()+1);
      for (int cellIndex = 0 ; cellIndex < l.size(); cellIndex++){
        Cell cell = row.createCell(cellIndex);
        setCell(cell, l.get(cellIndex), getCellStyle(0, cell, this.bodyStyle));
      }
    });
    postHandler(0, sheet, titleRows, dataRows[0]);
    return this.workbook;
  }

  private void postHandler(int sheetIndex, Sheet sheet , int titleRows, int dataRows){
    if (true == getList(this.autoCellWidth, sheetIndex, false)){
      if (sheet instanceof SXSSFSheet){
        ((SXSSFSheet)sheet).trackAllColumnsForAutoSizing();
      }
      Row row = sheet.getRow(sheet.getLastRowNum());
      for (int i = 0; i < row.getLastCellNum(); i++){
        sheet.autoSizeColumn(i);
      }
    }
    if (getList(this.cellConstraint, sheetIndex, Collections.EMPTY_MAP).size()>0){
      Map<Integer, List<String>> constraints = this.cellConstraint.get(sheetIndex);
      DataValidationHelper helper = sheet.getDataValidationHelper();
      for (Map.Entry<Integer, List<String>> entry: constraints.entrySet()){
        // 加载下拉列表内容
        String[] constraintValues = new String[entry.getValue().size()];
        entry.getValue().toArray(constraintValues);
        DataValidationConstraint constraint = helper.createExplicitListConstraint(constraintValues);
        // 设置数据有效性加载在哪个单元格上,四个参数分别是：起始行、终止行、起始列、终止列
        CellRangeAddressList regions = new CellRangeAddressList(titleRows, titleRows + dataRows, entry.getKey()-1, entry.getKey()-1);
        // 数据有效性对象
        DataValidation dataValidation = helper.createValidation(constraint, regions);
        dataValidation.setShowPromptBox(true);
        dataValidation.setEmptyCellAllowed(true);
        dataValidation.setErrorStyle(DataValidation.ErrorStyle.WARNING);
        sheet.addValidationData(dataValidation);
      }
    }
    int i = 0;
    if (getList(this.defaultWidths, sheetIndex, null)!=null){
      sheet.setDefaultColumnWidth(getList(this.defaultWidths, sheetIndex, null));
    } else if (defaultWidth!=null){
      sheet.setDefaultColumnWidth(defaultWidth);
    }
    for (Integer width: getList(this.columnWidths, sheetIndex, Collections.emptyList())) {
      i++;
      if (width == null) {
        continue;
      }
      sheet.setColumnWidth(i-1, width);
    }
  }

  public void export(File file) throws InvocationTargetException, IllegalAccessException, IOException, NoSuchMethodException {
    export(new FileOutputStream(file));
  }

  public void export(OutputStream out) throws InvocationTargetException, IllegalAccessException, IOException, NoSuchMethodException {
    generate();
    this.workbook.write(out);
    this.workbook.close();
  }

  /**
   *
   * @param sheet
   * @param title
   * @return 表头占用的行数
   */
  private int exportTitle(Sheet sheet, List<?> title, int sheetIndex, Boolean mergeHorizonFirst){
    if (title==null || title.size() == 0) {
      return 0;
    }
    int maxRowCount = 1;
    int length = title.size();
    List<Row> rows = new ArrayList<>();
    Row row = sheet.createRow(0);
    rows.add(row);
    int[] rowCount = new int[length];
    for (int i = 0; i < length; i++){
      Object titleCol = title.get(i);
      if (titleCol == null){
        rowCount[i] = 0;
        continue;
      }
      if (titleCol instanceof String){
        Cell cell = row.createCell(i);
        setCell(cell, titleCol, getCellStyle(sheetIndex, cell, this.titleStyle));
        rowCount[i] = 1;
      } else if (titleCol instanceof List){
         int curRowCount = ((List)titleCol).size();
         for (int j = rows.size(); j< curRowCount; j++){
           rows.add(sheet.createRow(j));
         }
         for (int j = 0; j < curRowCount; j++){
           Cell cell = rows.get(j).createCell(i);
           setCell(cell, ((List)titleCol).get(j), getCellStyle(sheetIndex, cell, this.titleStyle));
         }
         rowCount[i] = curRowCount;
         maxRowCount = curRowCount>maxRowCount?curRowCount:maxRowCount;
      }
    }
    if (maxRowCount == 1){
      return maxRowCount;
    } else {
      //复杂表头，需要检查是否需要合并单元格
      //判断是否需要纵向合并
      boolean verticalMerge = false;
      if (mergeHorizonFirst==null){
        mergeHorizonFirst = true;
      }
      for (int i = 1; i<length;i++){
        if (rowCount[i] != rowCount[i-1]){
          verticalMerge = true;
        }
      }
      boolean[][] isCellMerged = new boolean[maxRowCount][length];
      if (verticalMerge){
        for (int i = 0; i < length; i++){
          if (rowCount[i]<maxRowCount){
            //合并多余出来的单元格
            mergeCell(sheet, rowCount[i], i, maxRowCount-1, i);
            for (int k = rowCount[i]; k<=maxRowCount-1;k++){
              isCellMerged[k][i] = true;
            }
          }
        }
      }
      //找到相同的内容块做横向或者>=2×2的合并， 默认优先做水平合并。
      //优先垂直合并暂不做吧。真TM复杂
      int mergeStartRow = 0;
      int mergeStartCol = 0;
      int mergeEndRow = 0;
      int mergeEndCol = 0;
      int r = 0, c = 0;
      for (; r< maxRowCount; r++){
        mergeStartRow = r;
        mergeEndRow = r;
        while (true){
          while (c<length-1 && !isCellMerged[r][c] && !isCellMerged[r][c+1] && isCellEqual(title,r,c, r, c+1)){
            mergeEndCol = ++c;
          }
          if (mergeEndCol>mergeStartCol){
            //判断纵向是否需要合并
            for (int i = mergeStartRow; i< maxRowCount-1; i++){
              boolean needVerticalMerge = true;
              for (int j = mergeStartCol; j<=mergeEndCol; j++){
                needVerticalMerge = isCellEqual(title, i, j, i + 1, j);
                if (!needVerticalMerge){
                  break;
                }
              }
              if (needVerticalMerge){
                mergeEndRow = i+1;
              }else {
                break;
              }
            }
            mergeCell(sheet, mergeStartRow, mergeStartCol, mergeEndRow, mergeEndCol);
            for (int m=mergeStartRow; m <= mergeEndRow; m++){
              for (int n = mergeStartCol; n <= mergeEndCol; n++){
                isCellMerged[m][n] = true;
              }
            }
          } else if (r < maxRowCount-1){
            //横向不需合并，判断纵向是否需要合并
            int tmpR = r;
            while (tmpR<maxRowCount-1 &&!isCellMerged[tmpR][c] && !isCellMerged[tmpR+1][c] && isCellEqual(title, tmpR, c, tmpR+1, c)){
              mergeEndRow = ++tmpR;
            }
            if (mergeEndRow>mergeStartRow){
              mergeCell(sheet, mergeStartRow, mergeStartCol, mergeEndRow, mergeEndCol);
              for (int m=mergeStartRow; m <= mergeEndRow; m++){
                for (int n = mergeStartCol; n <= mergeEndCol; n++){
                  isCellMerged[m][n] = true;
                }
              }
            }
          }
          c++;
          while (c<length && isCellMerged[r][c]){
            c++;
          }
          if (c>=length){
            mergeStartCol = 0;
            mergeEndCol = 0;
            c=0;
            break;
          } else {
            mergeStartCol = c;
            mergeEndCol = c;
          }
          mergeEndRow = r;
        }
      }
    }
    return maxRowCount;
  }

  private<E> List<E> setList(List<E> list, int index, E data, E fillEmpty, int listCapacity){
    if (list == null){
      list = new ArrayList<E>(listCapacity);
    }
    if (index<0){
      list.add(data);
      return list;
    }
    if (index<list.size()){
      list.set(index, data);
      return list;
    }
    for (int i = list.size(); i < index; i++){
      list.add(fillEmpty);
    }
    list.add(data);
    return list;
  }

  private<E> List<E> setList(List<E> list, E data, int initialCapacity){
    return setList(list, -1, data, null, initialCapacity);
  }

  private<E> List<E> setList(List<E> list, int index, E data, E fillEmpty){
    return setList(list, index, data, fillEmpty, index);
  }

  private <E> E getList(List<E> list, int index, E defaultValue){
    if (list == null || index>=list.size()){
      return defaultValue;
    } else if (index<0){
      if (list == null || list.isEmpty() ) return defaultValue;
      return list.get(list.size()-1);
    }
    E r = list.get(index);
    if (r == null) {
      return defaultValue;
    }
    return r;
  }

  private <E> E getList(List<E> list, int index) {
    return getList(list, index, null);
  }

  private boolean isCellEqual(List<?> title, int firstCellRow, int firstCellCol, int secondCellRow, int secondCellCol){
    if (((List)title.get(firstCellCol)).get(firstCellRow)==null){
      if (((List)title.get(secondCellCol)).get(secondCellRow)==null){
        return true;
      }
      return false;
    }
    return ((List)title.get(firstCellCol)).get(firstCellRow).equals(((List)title.get(secondCellCol)).get(secondCellRow));
  }

  private int mergeCell(Sheet sheet, int firstRow, int firstCol, int lastRow, int lastCol){
    if (lastRow > firstRow || lastCol > firstCol){
      sheet.addMergedRegion(new CellRangeAddress(firstRow, lastRow, firstCol, lastCol));
      return (lastRow-firstRow+1)*(lastCol-firstCol+1);
    }
    return 0;
  }

  private List<Object> flatData(Object data, Class type, List<String> sortedFields, Map<String, Method> beanGetters) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    if (List.class.isAssignableFrom(type)){
      return (List<Object>) data;
    } else if (Map.class.isAssignableFrom(type)){
      List<Object> result = new ArrayList<>(this.sortedFields.size());
      for (String key: sortedFields){
        if (key==null||"".equals(key.trim())){
          result.add(null);
        } else {
          result.add(((Map)data).get(key));
        }
      }
      return result;
    } else {
      List<Object> result = new ArrayList<>(this.sortedFields.size());
      for (String key: sortedFields){
        if (key==null||"".equals(key.trim())){
          result.add(null);
        } else {
          result.add(beanGetters.getOrDefault(key, BeanUtil.getBeanGetter(type, key)).invoke(data));
        }
      }
      return result;
    }
  }

  private void setCell(Cell cell, Object data){
    setCell(cell, data, null);
  }

  private int setCell(Cell cell, Object data, CellStyle style){
    if (data == null){
      return 0;
    } if (data instanceof CellData) {
      return setCell(cell, ((CellData) data).getCellData(), mergeCellStyle(style, ((CellData) data).getCellStyle(this)));
    } else if (data instanceof Long) {
      cell.setCellValue(((Long) data).toString());
    } else if (data instanceof Number){
      cell.setCellValue(((Number)data).doubleValue());
    } else if (data instanceof String){
      cell.setCellValue((String)data);
    } else if (data instanceof Boolean){
      cell.setCellValue((boolean)data);
    } else if (data instanceof Date){
      //cell.setCellValue(dateFormat.format(data));
      cell.setCellValue((Date) data);
      if (dataFormat==null){
        dataFormat = this.workbook.getCreationHelper().createDataFormat();
      }
      if (style == null){
        style = this.workbook.createCellStyle();
      }
      style.setDataFormat(dataFormat.getFormat(datetimeFormat));
    } else if (data instanceof Calendar){
      cell.setCellValue(((Calendar)data));
    } else if (data instanceof Serializable){
      cell.setCellValue(data.toString());
    } else {
      return 0;
    }
    if (style!=null){
      cell.setCellStyle(style);
    }
    return 1;
  }

  /**
   * style合并可能会产生意想不到的结果
   * @param styleTo
   * @param styleFrom
   */
  private CellStyle mergeCellStyle(CellStyle styleFrom, CellStyle styleTo){
    if (styleFrom == null) return styleTo;
    else if (styleTo == null) return styleFrom;
    if (styleTo.getBorderBottom().equals(BorderStyle.NONE) && !styleFrom.getBorderBottom().equals(BorderStyle.NONE)){
      styleTo.setBorderBottom(styleFrom.getBorderBottom());
    }
    if (styleTo.getBorderTop().equals(BorderStyle.NONE) && !styleFrom.getBorderTop().equals(BorderStyle.NONE)){
      styleTo.setBorderTop(styleFrom.getBorderTop());
    }
    if (styleTo.getBorderLeft().equals(BorderStyle.NONE) && !styleFrom.getBorderLeft().equals(BorderStyle.NONE)){
      styleTo.setBorderLeft(styleFrom.getBorderLeft());
    }
    if (styleTo.getBorderRight().equals(BorderStyle.NONE) && !styleFrom.getBorderRight().equals(BorderStyle.NONE)){
      styleTo.setBorderRight(styleFrom.getBorderRight());
    }
    if (styleTo.getAlignment().equals(HorizontalAlignment.GENERAL) && !styleFrom.getAlignment().equals(HorizontalAlignment.GENERAL)){
      styleTo.setAlignment(styleFrom.getAlignment());
    }
    if (styleTo.getVerticalAlignment().equals(VerticalAlignment.BOTTOM) && !styleFrom.getVerticalAlignment().equals(VerticalAlignment.BOTTOM)) {
      styleTo.setVerticalAlignment(styleFrom.getVerticalAlignment());
    }
    if (styleTo.getFillBackgroundColor() == IndexedColors.AUTOMATIC.index && styleFrom.getFillBackgroundColor() != IndexedColors.AUTOMATIC.index){
      styleTo.setFillBackgroundColor(styleFrom.getFillBackgroundColor());
    }
    if (styleTo.getFillForegroundColor() == IndexedColors.AUTOMATIC.index && styleFrom.getFillForegroundColor() != IndexedColors.AUTOMATIC.index) {
      styleTo.setFillForegroundColor(styleFrom.getFillForegroundColor());
    }
    return styleTo;
  }

  private CellStyle getCellStyle(int sheetIndex, Cell cell, CellStyle defaultStyle){
    Pair<List<? extends Pair<Integer, Integer>>, List<? extends Pair<Integer, Integer>>> customStyleRange = getList(this.customSytleRanges, sheetIndex, null);
    if (customStyleRange == null) { return defaultStyle; }
    boolean hit = false;
    for (Pair<Integer, Integer> pair : customStyleRange.getLeft()) {
      if (cell.getColumnIndex() < pair.getLeft()) {
        return defaultStyle;
      } else if (cell.getColumnIndex() <= pair.getRight())  {
        hit = true;
        break;
      }
    }
    if (!hit) { return defaultStyle; }
    hit = false;
    for (Pair<Integer, Integer> pair : customStyleRange.getRight()) {
      if (cell.getRowIndex() < pair.getLeft()) {
        return defaultStyle;
      } else if (cell.getRowIndex() <= pair.getRight())  {
        hit = true;
        break;
      }
    }
    if (!hit) { return defaultStyle; }

    Map<CellRangeAddress, CellStyle> cellStyleMap = getList(this.customStyle, sheetIndex, Collections.emptyMap());
    if (cellStyleMap.size()>0){
      CellStyle style = cellStyleMap.entrySet().stream().filter(i->i.getKey().isInRange(cell)).map(i->i.getValue()).reduce(defaultStyle, (a,b)->mergeCellStyle(a,b));
      return style;
    } else {
      return defaultStyle;
    }
  }

  private static abstract class BeanUtil {

    public static Method findMethod(Class<?> clazz, String methodName, Class<?>... paramTypes) {
      try {
        return clazz.getMethod(methodName, paramTypes);
      } catch (NoSuchMethodException var4) {
        return findDeclaredMethod(clazz, methodName, paramTypes);
      }
    }

    public static Method findDeclaredMethod(Class<?> clazz, String methodName, Class<?>... paramTypes) {
      try {
        return clazz.getDeclaredMethod(methodName, paramTypes);
      } catch (NoSuchMethodException var4) {
        return clazz.getSuperclass() != null ? findDeclaredMethod(clazz.getSuperclass(), methodName, paramTypes) : null;
      }
    }

    public static List<Method> getBeanGetters(Class<?> clazz) {
      Class cl = clazz;
      List<Method> methodList = new LinkedList<>();
      while (cl != null && !cl.equals(Object.class)) {
        Method[] methods = cl.getDeclaredMethods();
        for (Method m : methods) {
          if (m.getParameterCount() > 0){
            continue;
          }
          if (Modifier.isProtected(m.getModifiers())
                  || Modifier.isAbstract(m.getModifiers())
                  ||Modifier.isPrivate(m.getModifiers())){
            continue;
          }
          if (m.getName().startsWith("get")) {
            methodList.add(m);
          } /*else if (m.getName().startsWith("is") && m.getReturnType().equals(Boolean.class)){
            methodList.add(m);
          }*/
        }
        cl = cl.getSuperclass();
      }
      return methodList;
    }

    public static Method getBeanGetter(Class<?> clazz, String fieldName) throws NoSuchMethodException {
      return getBeanGetter(clazz, fieldName, true);
    }

    private static Method getBeanGetter(final Class<?> clazz,final String fieldName,final boolean captured) throws NoSuchMethodException {
      Class cl = clazz;
      String field = fieldName;
      if (captured) {field = capture(fieldName);}
      while (cl != null && !Object.class.equals(cl)) {
        try {
          return cl.getDeclaredMethod("get" + field);
        } catch (NoSuchMethodException var5) {
          cl = cl.getSuperclass();
        }
      }
      if (captured){
        return getBeanGetter(clazz, fieldName, false);
      } else {
        throw new NoSuchMethodException("No Such Field:" + fieldName);
      }
    }

    public static String capture(String str) {
      char[] chars = str.toCharArray();
      if (chars.length == 0) {
        return str;   //同一对象
      } else if (chars[0] >= 'a' && chars[0] <= 'z') {
        chars[0] -= 'a' - 'A';
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
        chars[0] += 'a' - 'A';
        return new String(chars);   //新对象
      } else {
        return str;   //同一对象
      }
    }
  }

  public static<T> List<List<T>> transpose(List<List<T>> data){
    if (data.size()==0){
      return data;
    }
    int rows = data.size();
    int cols = data.stream().map(List::size).max(Comparator.comparingInt(i -> i)).get();
    List<List<T>> result = new ArrayList<>(cols);
    for (int i = 0; i < cols; i++ ){
      result.add(new ArrayList<>(rows));
      for (int j = 0; j < rows; j++){
        result.get(i).add(null);
      }
    }
    for (int i = 0; i < rows; i++){
      List<T> line = data.get(i);
      for (int j = 0; j < cols; j++){
        result.get(j).set(i, line.get(j));
      }
    }
    return result;
  }

  public static class CellData {
    private Object cellData;

    private IndexedColors backgroundColor;
    private IndexedColors foregroundColor;

    private BorderStyle borderTop;
    private BorderStyle borderRight;
    private BorderStyle borderLeft;
    private BorderStyle borderBottom;

    private Short borderTopColor;
    private Short borderRightColor;
    private Short borderLeftColor;
    private Short borderBottomColor;

    private HorizontalAlignment horizontalAlignment;
    private VerticalAlignment verticalAlignment;

    private Boolean hidden;

    private Boolean locked;

    private Short rotation;

    private Short indent;

    private FillPatternType fillPattern;

    private String fontName;

    private Boolean fontBold;

    private Byte fontUnderline;

    private Boolean fontItalic;

    private Short fontHeightInPoints;

    private Short fontColor;

    private CellData(Object data){
      this.cellData = data;
    }

    public static CellData of(Object cellData) {
      return new CellData(cellData);
    }

    public Object getCellData() {
      return cellData;
    }


    protected CellStyle getCellStyle(ExcelWriter writer) {
      CellStyle cellStyle;
      if ( (cellStyle = writer.cellDataStyleCache.get(this)) == null ) {
        cellStyle = writer.workbook.createCellStyle();
        writer.cellDataStyleCache.put(this, cellStyle);
      }  else {
        return cellStyle;
      }
      if (this.fontName != null
              || this.fontBold != null
              || this.fontUnderline != null
              || this.fontItalic != null
              || this.fontHeightInPoints != null
              || this.fontColor != null
      ) {
        Font font = writer.workbook.createFont();
        if (this.fontName!= null) {
          font.setFontName(this.fontName);
        }
        if (this.fontBold != null) {
          font.setBold(this.fontBold);
        }
        if (this.fontUnderline != null) {
          font.setUnderline(this.fontUnderline);
        }
        if (this.fontItalic != null) {
          font.setItalic(this.fontItalic);
        }
        if (this.fontHeightInPoints != null) {
          font.setFontHeightInPoints(this.fontHeightInPoints);
        }
        if (this.fontColor != null) {
          font.setColor(this.fontColor);
        }
        cellStyle.setFont(font);
      }
      if (this.backgroundColor != null) {
        cellStyle.setFillBackgroundColor(this.backgroundColor.getIndex());
      }
      if (this.foregroundColor != null) {
        cellStyle.setFillForegroundColor(this.foregroundColor.getIndex());
      }
      if (this.borderTop != null) {
        cellStyle.setBorderTop(this.borderTop);
      }
      if (this.borderRight != null) {
        cellStyle.setBorderRight(this.borderRight);
      }
      if (this.borderLeft != null) {
        cellStyle.setBorderLeft(this.borderLeft);
      }
      if (this.borderBottom != null) {
        cellStyle.setBorderBottom(this.borderBottom);
      }
      if (this.borderTopColor != null) {
        cellStyle.setTopBorderColor(this.borderTopColor);
      }
      if (this.borderRightColor != null) {
        cellStyle.setRightBorderColor(this.borderRightColor);
      }
      if (this.borderLeftColor != null ) {
        cellStyle.setLeftBorderColor(this.borderLeftColor);
      }
      if (this.borderBottomColor != null ) {
        cellStyle.setBottomBorderColor(this.borderBottomColor);
      }
      if (this.horizontalAlignment != null) {
        cellStyle.setAlignment(this.horizontalAlignment);
      }
      if (this.verticalAlignment != null) {
        cellStyle.setVerticalAlignment(this.verticalAlignment);
      }
      if (this.hidden != null) {
        cellStyle.setHidden(this.hidden);
      }
      if (this.locked != null) {
        cellStyle.setLocked(this.locked);
      }
      if (this.rotation != null) {
        cellStyle.setRotation(this.rotation);
      }
      if (this.indent != null ) {
        cellStyle.setIndention(this.indent);
      }
      if (this.fillPattern != null ) {
        cellStyle.setFillPattern(this.fillPattern);
      }
      return cellStyle;
    }

    public CellData setCellData(Object cellData) {
      this.cellData = cellData;
      return this;
    }

    public CellData setBackgroundColor(IndexedColors backgroundColor) {
      this.backgroundColor = backgroundColor;
      return this;
    }

    public CellData setForegroundColor(IndexedColors foregroundColor) {
      this.foregroundColor = foregroundColor;
      return this;
    }

    public CellData setBorderTop(BorderStyle borderTop) {
      this.borderTop = borderTop;
      return this;
    }

    public CellData setBorderRight(BorderStyle borderRight) {
      this.borderRight = borderRight;
      return this;
    }

    public CellData setBorderLeft(BorderStyle borderLeft) {
      this.borderLeft = borderLeft;
      return this;
    }

    public CellData setBorderBottom(BorderStyle borderBottom) {
      this.borderBottom = borderBottom;
      return this;
    }

    public CellData setBorderTopColor(Short borderTopColor) {
      this.borderTopColor = borderTopColor;
      return this;
    }

    public CellData setBorderRightColor(Short borderRightColor) {
      this.borderRightColor = borderRightColor;
      return this;
    }

    public CellData setBorderLeftColor(Short borderLeftColor) {
      this.borderLeftColor = borderLeftColor;
      return this;
    }

    public CellData setBorderBottomColor(Short borderBottomColor) {
      this.borderBottomColor = borderBottomColor;
      return this;
    }

    public CellData setHorizontalAlignment(HorizontalAlignment horizontalAlignment) {
      this.horizontalAlignment = horizontalAlignment;
      return this;
    }

    public CellData setVerticalAlignment(VerticalAlignment verticalAlignment) {
      this.verticalAlignment = verticalAlignment;
      return this;
    }

    public CellData setHidden(Boolean hidden) {
      this.hidden = hidden;
      return this;
    }

    public CellData setLocked(Boolean locked) {
      this.locked = locked;
      return this;
    }

    public CellData setRotation(Short rotation) {
      this.rotation = rotation;
      return this;
    }

    public CellData setIndent(Short indent) {
      this.indent = indent;
      return this;
    }

    public CellData setFillPattern(FillPatternType fillPattern) {
      this.fillPattern = fillPattern;
      return this;
    }

    public CellData setFontName(String fontName) {
      this.fontName = fontName;
      return this;
    }

    public CellData setFontBold(Boolean fontBold) {
      this.fontBold = fontBold;
      return this;
    }

    public CellData setFontUnderline(Byte fontUnderline) {
      this.fontUnderline = fontUnderline;
      return this;
    }

    public CellData setFontItalic(Boolean fontItalic) {
      this.fontItalic = fontItalic;
      return this;
    }

    public CellData setFontHeightInPoints(Short fontHeightInPoints) {
      this.fontHeightInPoints = fontHeightInPoints;
      return this;
    }

    public CellData setFontColor(Short fontColor) {
      this.fontColor = fontColor;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CellData cellData = (CellData) o;
      return borderTopColor == cellData.borderTopColor &&
              borderRightColor == cellData.borderRightColor &&
              borderLeftColor == cellData.borderLeftColor &&
              borderBottomColor == cellData.borderBottomColor &&
              hidden == cellData.hidden &&
              locked == cellData.locked &&
              rotation == cellData.rotation &&
              indent == cellData.indent &&
              fontBold == cellData.fontBold &&
              fontUnderline == cellData.fontUnderline &&
              fontItalic == cellData.fontItalic &&
              fontHeightInPoints == cellData.fontHeightInPoints &&
              fontColor == cellData.fontColor &&
              backgroundColor == cellData.backgroundColor &&
              foregroundColor == cellData.foregroundColor &&
              borderTop == cellData.borderTop &&
              borderRight == cellData.borderRight &&
              borderLeft == cellData.borderLeft &&
              borderBottom == cellData.borderBottom &&
              horizontalAlignment == cellData.horizontalAlignment &&
              verticalAlignment == cellData.verticalAlignment &&
              fillPattern == cellData.fillPattern &&
              Objects.equals(fontName, cellData.fontName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(backgroundColor, foregroundColor, borderTop, borderRight, borderLeft, borderBottom, borderTopColor, borderRightColor, borderLeftColor, borderBottomColor, horizontalAlignment, verticalAlignment, hidden, locked, rotation, indent, fillPattern, fontName, fontBold, fontUnderline, fontItalic, fontHeightInPoints, fontColor);
    }
  }

  private static class Pair<L, R> implements Map.Entry<L, R>, Serializable {

    /** Serialization version */
    private static final long serialVersionUID = 4954918890077093841L;

    private L left;
    private R right;

    public static <L, R> Pair<L, R> of(final L left,final R right) {
      return new Pair<>(left, right);
    }

    public Pair(final L left, final R right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public L getKey() {
      return left;
    }

    @Override
    public R getValue() {
      return right;
    }

    public L getLeft() {
      return left;
    }

    public R getRight() {
      return right;
    }

    @Override
    public R setValue(R value) {
      this.right = value;
      return value;
    }

    public void setLeft(L left) {
      this.left = left;
    }

    public void setRight(R right) {
      this.right = right;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof Map.Entry<?, ?>) {
        final Map.Entry<?, ?> other = (Map.Entry<?, ?>) obj;
        return Objects.equals(getKey(), other.getKey())
                && Objects.equals(getValue(), other.getValue());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (getKey() == null ? 0 : getKey().hashCode()) ^
              (getValue() == null ? 0 : getValue().hashCode());
    }

    @Override
    public String toString() {
      return "(" + getLeft() + ',' + getRight() + ')';
    }
  }
}
