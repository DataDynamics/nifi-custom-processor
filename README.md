# NiFi Custom Services

이 프로젝트는 NiFi에서 사용할 수 있는 Custom Processor, Controller Service, Reporting Task 등을 구현한 NiFi 프로젝트입니다.

## Requirement

* NiFi 1.18.0
* Apache Maven 3.8.0 이상
* JDK 11 이상

## Build

```
# mvn clean package
```

## NiFi 로그인 인증

NiFi 최초 설치시 SSL을 위한 구성이 자동으로 진행되며 이 경우 단일 사용자를 위한 로그인을 다음의 커맨드를 이용하여 설정합니다.

```
# cd <NIFI_HOME>/bin
# sh nifi.sh set-single-user-credentials admin adminadminadmin
```

## NAR 파일 배포

빌드한 NAR 파일을 배포하려면 다음과 같이 NAR 파일을 복사합니다.
소스코드를 수정하고 재배포하는 경우에도 동일하게 진행하면 변경내용을 확인하여 NiFi가 내부적으로 NAR를 재배포합니다.

```
# cp nifi-custom-reporting-tasks-1.0.6.nar <NIFI_HOME>/extentions
# cd <NIFI_HOME>/bin
# sh nifi.sh start
```

## Processor

### Custom Timestamp Pattern Put Kudu

기존 Put Kudu는 다음의 문제점이 있습니다.

* Timestamp를 UTC 기준으로 처리
* Timestamp Pattern 처리에 대한 기능 부족

따라서 Custom Timestamp Pattern Put Kudu 모듈은 다음을 추가로 지원합니다.

* String Timestamp 컬럼에 대해서 Timestamp Format을 커스텀하게 지정
* Timestamp의 경우 Timestamp, Timestamp Millis, Timestamp Micro를 지원
* PutKudu에서 Timestamp의 경우 UTC로 날짜가 변환되는 것을 조정할 수 있는 기능 추가

테스트를 위해서 CSV 파일을 다음과 같이 작성합니다.

```
COL_INT,COL_FLOAT,COL_TIMESTAMP,COL_TIMESTAMP_MILLIS,COL_TIMESTAMP_MICROS
1,1.1,2022-11-11 11:11:11,2022-11-11 11:11:11.111,2022-11-11 11:11:111111
```

해당 CSV 파일의 Avro Schema는 다음과 같으며 Timstamp는 문자열로 우선 처리를 하게 되므로 string으로 자료형을 지정합니다.

```json
{
  "namespace": "nifi",
  "type": "record",
  "name": "input",
  "fields": [
    {
      "name": "col_int",
      "type": ["null", "int"]
    },
    {
      "name": "col_float",
      "type": ["null", "float"]
    },
    {
      "name": "col_timestamp",
      "type": ["null", {"type": "string", "logicalType": "timestamp-millis"}]
    },
    {
      "name": "col_timestamp_millis",
      "type": ["null", {"type": "string", "logicalType": "timestamp-millis"}]
    },
    {
      "name": "col_timestamp_micros",
      "type": ["null", {"type": "string", "logicalType": "timestamp-micros"}]
    }
  ]
}
```

이제 데이터를 저장할 Kudu 테이블을 생성합니다.

```sql
CREATE TABLE input
(
    col_int INT,
    col_float FLOAT,
    col_timestamp TIMESTAMP,
    col_timestamp_millis TIMESTAMP,
    col_timestamp_micros TIMESTAMP,
    PRIMARY KEY(col_int)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU;
```

이제 Timestamp 컬럼에 대해서 문자열 Timestamp 컬럼을 파싱하기 위한 Timestamp Pattern을 다음과 같이 정의합니다.

```json
{
  "formats": [
    {
      "column-name": "col_timestamp",
      "timestamp-pattern": "yyyy-MM-dd HH:mm:ss",
      "type": "TIMESTAMP_MILLIS"
    },
    {
      "column-name": "col_timestamp_millis",
      "timestamp-pattern": "yyyy-MM-dd HH:mm:ss.SSS",
      "type": "TIMESTAMP_MILLIS"
    },
    {
      "column-name": "col_timestamp_micros",
      "timestamp-pattern": "yyyy-MM-dd HH:mm:ss.SSSSSS",
      "type": "TIMESTAMP_MICROS"
    }
  ]
}
```

이제 Put Kudu를 실행하면 다음과 같이 UTC가 적용되어 -9시간으로 시간이 변경됩니다.

```
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
| col_int | col_float         | col_timestamp       | col_timestamp_millis          | col_timestamp_micros          |
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
| 1       | 1.100000023841858 | 2022-11-11 02:11:11 | 2022-11-11 02:11:11.111000000 | 2022-11-11 02:11:11.111111000 |
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
```

Timestamp 컬럼의 시간을 +9로 입력하면 이제 11시로 정상적으로 표시됩니다.

```
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
| col_int | col_float         | col_timestamp       | col_timestamp_millis          | col_timestamp_micros          |
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
| 1       | 1.100000023841858 | 2022-11-11 11:11:11 | 2022-11-11 11:11:11.111000000 | 2022-11-11 11:11:11.111111000 |
+---------+-------------------+---------------------+-------------------------------+-------------------------------+
```

### DatabaseProcessor

DBCP Connection Pool을 이용한 SQL을 실행시키는 예제 Processor입니다.

#### Impala JDBC Driver

Impala JDBC Driver로 테스트를 위해서 `lib/ImpalaJDBC42.jar` 파일을 NIFI가 설치되어 있는 적정 위치(예; `/opt/cloudera/parcels/CFM/NIFI/lib`)에
업로드합니다.

* JDBC Driver는 `com.cloudera.impala.jdbc.Driver`
* JDBC URL은 `jdbc:impala://host:21050/default`

### Bulk Oracle Insert Processor

Bulk Oracle Insert Processor는 Record Reader를 통해서 수신한 Record를 Avro Parser를 통해 Avro Schema를 확인하여 다음의 Bulk Insert를 위한 SQL을
생성해서 INSERT합니다.

```
INSERT ALL
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( false, 1, 123123, 123123.0, 3.14, 'Hello World', 11.11, 19575, 32051998, 32051998834, 1691279651998, '2022-11-11 11:11:11.111' )
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( false, 1, 123123, 123123.0, 3.14, 'Hello World', 11.11, 19575, 32051998, 32051998834, 1691279651998, '2022-11-11 11:11:11.111' )
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( false, 1, 123123, 123123.0, 3.14, 'Hello World', 11.11, 19575, 32051998, 32051998834, 1691279651998, '2022-11-11 11:11:11.111' )
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( 'NULL', 1, 'NULL', 'NULL', 'NULL', 'Hello World', 11.11, 19575, 32051999, 'NULL', 1691279651999, '2022-11-11 11:11:11.111' )
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( 'NULL', 1, 'NULL', 'NULL', 'NULL', 'Hello World', 11.11, 19575, 32051999, 'NULL', 1691279651999, '2022-11-11 11:11:11.111' )
INTO schema1.table1 ( TypeBoolean, TypeInt, TypeLong, TypeFloat, TypeDouble, TypeString, TypeBytesDecimal, TypeDate, TypeTimeInMillis, TypeTimeInMicros, TypeTimestampInMillis, TypeStringTimestampInMillis ) VALUES ( 'NULL', 1, 'NULL', 'NULL', 'NULL', 'Hello World', 11.11, 19575, 32051999, 'NULL', 1691279651999, '2022-11-11 11:11:11.111' )
SELECT 1 FROM dual;
```

## Reporting Tasks

기존에 NiFi에 포함되어 있는 Reporting Task의 경우 로그 메시지 출력을 통해 NiFi UI의 Bulletin에 표시되도록 할 수는 있습니다.
하지만 장애가 발생할 수 있는 중요한 정보는 알람이 필요한 경우가 많으므로 다음의 Reporting Task의 경우 외부 HTTP URI에 임계치를 초과하는 경우 알람을 발송할 수 있습니다.

* `MonitorDiskUsageReportingTask` - Disk Usage의 지정한 임계치를 초과한 경우 특정 URI로 알람을 전송합니다.
* `MonitorMemoryPoolReportingTask` - JVM Memory Pool의 지정한 임계치를 초과한 경우 특정 URI로 알람을 전송합니다.
* `MonitorMemoryUsageReportingTask` - JVM Memory Usageㅇ의 지정한 임계치를 초과한 경우 특정 URI로 알람을 전송합니다.
* `MonitorThreadReportingTask` - JVM Thread의 지정한 임계치를 초과한 경우 특정 URI로 알람을 전송합니다.

## 기타

### Apache Ant

`build.xml` 파일에서 SCP를 수행하기 위해서 IntelliJ IDEA에서 실행하는 경우 다음과 같이 JSCH가 필요합니다.

```
# wget https://repo1.maven.org/maven2/com/jcraft/jsch/0.1.55/jsch-0.1.55.jar
# mkdir -p ~/.ant/lib
# mv jsch-0.1.55.jar ~/.ant/lib
```
