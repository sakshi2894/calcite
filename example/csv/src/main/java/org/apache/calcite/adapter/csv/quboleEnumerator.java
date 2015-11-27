package org.apache.calcite.adapter.csv;

import com.google.common.collect.ImmutableList;
import com.qubole.qds.sdk.java.client.DefaultQdsConfiguration;
import com.qubole.qds.sdk.java.client.QdsClient;
import com.qubole.qds.sdk.java.client.QdsClientFactory;
import com.qubole.qds.sdk.java.client.QdsConfiguration;
import com.qubole.qds.sdk.java.client.ResultLatch;
import com.qubole.qds.sdk.java.entities.CommandResponse;
import com.qubole.qds.sdk.java.entities.ResultValue;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

/**
 * Created by dev on 11/23/15.
 */
public class quboleEnumerator<E> implements Enumerator<E>{

  private ImmutableList<ImmutableList<String>> data;
  private Iterator<ImmutableList<String>> iterator;

  private final RowConverter<E> rowConverter;
  //private ImmutableList<String> current;
  private E current;

  private static QdsConfiguration configuration;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("hh:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance(
            "yyyy-MM-dd hh:mm:ss", gmt);
  }

  quboleEnumerator(File file, RowConverter<E> rowConverter) throws Exception {

    this.rowConverter = rowConverter;

    configuration = new DefaultQdsConfiguration("url", "password");
    QdsClient client  = QdsClientFactory.newClient(configuration);
    String sql = new String("select * from tpcds_orc_500.customer LIMIT 10;");
    CommandResponse commandResponse = client.command().hive().query(sql).invoke().get();

    ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
    ResultValue resultValue = resultLatch.awaitResult();

    String[] result = resultValue.getResults().split("\t");

    ImmutableList.Builder<ImmutableList<String>> dataBuilder = new ImmutableList.Builder<ImmutableList<String>>();

    ImmutableList.Builder<String> rowBuilder = new ImmutableList.Builder<String>();

    ImmutableList<String> current;

    for(int i = 0 ; i < result.length; i++) {

      if(result[i].contains("\r\n")) {

        String[] end = result[i].split("\r\n");
        rowBuilder.add(end[0]);

        current = rowBuilder.build();
        dataBuilder.add(current);

        rowBuilder = new ImmutableList.Builder<String>();
        if(end.length==2) rowBuilder.add(end[1]);
      }

      else {
        rowBuilder.add(result[i]);
      }
    }
    data = dataBuilder.build();
    iterator = data.iterator();

  }

  public quboleEnumerator(File file, List<CsvFieldType> fieldTypes) throws Exception {
    this(file, fieldTypes, identityList(fieldTypes.size()));
  }

  public quboleEnumerator(File file, List<CsvFieldType> fieldTypes, int[] fields) throws Exception {
    //noinspection unchecked
    this(file, (RowConverter<E>) converter(fieldTypes, fields));
  }
  @Override
  public E current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    for(;;) {
      if (iterator.hasNext() == false) {
        current = null;
        return false;
      } else {
        ImmutableList<String> current_immutableList = iterator.next();
        String[] strings = new String[current_immutableList.size()];

        int i = 0;
        for (String curr : current_immutableList) {
          strings[i] = curr;
          i++;
        }
        current = rowConverter.convertRow(strings);

        return true;
      }
    }

  }

  private static RowConverter<?> converter(List<CsvFieldType> fieldTypes,
                                           int[] fields) {
    if (fields.length == 1) {
      final int field = fields[0];
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return new ArrayRowConverter(fieldTypes, fields);
    }
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file. */

  static RelDataType deduceRowType(JavaTypeFactory typeFactory, File file,
                                   List<CsvFieldType> fieldTypes) {
    ImmutableList.Builder<Map<String, String>> headersBuilder = new ImmutableList.Builder<Map<String, String>>();
    ImmutableList<Map<String, String>> headers;
    final List<RelDataType> types = new ArrayList<RelDataType>();
    final List<String> names = new ArrayList<String>();

    try {
      configuration = new DefaultQdsConfiguration("url", "password");
      QdsClient client = QdsClientFactory.newClient(configuration);
      ResultValue resultValue;
      try {
        String sql = new String("describe tpcds_orc_500.customer;");
        CommandResponse commandResponse = client.command().hive().query(sql).invoke().get();
        ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
        resultValue = resultLatch.awaitResult();
      } finally {
        client.close();
      }

      String[] result = resultValue.getResults().split("\r\n");

      for (int i = 0; i < result.length; i++) {
        String[] entry = result[i].split("\t");
        Map<String, String> nameType = new LinkedHashMap<String, String>();
        nameType.put(entry[0].trim(), entry[1].trim());
        headersBuilder.add(nameType);
      }

      headers =  headersBuilder.build();

      for(int i = 0; i < headers.size(); i ++) {
        Map<String, String> entry = headers.get(i);
        final String name;
        final CsvFieldType fieldType;
        String key = "";
        for (String key_set : entry.keySet()) {
          key = key_set;
        }
        name = key;
        String typeString = entry.get(name);

        fieldType = CsvFieldType.of(typeString);

        final RelDataType type;
        type = fieldType.toType(typeFactory);

        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createJavaType(String.class));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  abstract static class RowConverter<E> {
    abstract E convertRow(String[] rows);

    protected Object convert(CsvFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
        case BOOLEAN:
          if (string.length() == 0) {
            return null;
          }
          return Boolean.parseBoolean(string);
        case BYTE:
          if (string.length() == 0) {
            return null;
          }
          return Byte.parseByte(string);
        case SHORT:
          if (string.length() == 0) {
            return null;
          }
          return Short.parseShort(string);
        case INT:
          if (string.length() == 0) {
            return null;
          }
          return Integer.parseInt(string);
        case LONG:
          if (string.length() == 0) {
            return null;
          }
          return Long.parseLong(string);
        case FLOAT:
          if (string.length() == 0) {
            return null;
          }
          return Float.parseFloat(string);
        case DOUBLE:
          if (string.length() == 0) {
            return null;
          }
          return Double.parseDouble(string);
        case DATE:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_DATE.parse(string);
            return new java.sql.Date(date.getTime());
          } catch (ParseException e) {
            return null;
          }
        case TIME:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_TIME.parse(string);
            return new java.sql.Time(date.getTime());
          } catch (ParseException e) {
            return null;
          }
        case TIMESTAMP:
          if (string.length() == 0) {
            return null;
          }
          try {
            Date date = TIME_FORMAT_TIMESTAMP.parse(string);
            return new java.sql.Timestamp(date.getTime());
          } catch (ParseException e) {
            return null;
          }
        case STRING:
        default:
          return string;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final CsvFieldType[] fieldTypes;
    private final int[] fields;

    ArrayRowConverter(List<CsvFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new CsvFieldType[fieldTypes.size()]);
      this.fields = fields;
    }

    public Object[] convertRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter {
    private final CsvFieldType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(CsvFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }

  @Override
  public void reset() {
    iterator = data.iterator();
  }

  @Override
  public void close() {

  }
}
