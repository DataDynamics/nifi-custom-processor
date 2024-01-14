/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.parquet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

@Tags({"custom", "hadoop", "HDFS", "merge", "parquet", "source", "filesystem"})
@CapabilityDescription("HDFS에서 Parquet 파일을 로딩하여 Merge 후에 저장한다. 이 프로세서는 Merge후 원본 파일을 삭제한다.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "생성한 Parquet 파일의 파일명"),
        @WritesAttribute(attribute = "path", description = "Parquet 파일을 저장한 디렉토리")})
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.READ_DISTRIBUTED_FILESYSTEM, explanation = "HDFS 또는 로컬 파일 시스템의 읽기 접근 권한이 필요합니다."),
        @Restriction(requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM, explanation = "HDFS 또는 로컬 파일 시스템의 쓰기 접근 권한이 필요합니다.")
})
public class MergeParquet extends AbstractProcessor {

    ///////////////////////////////////////////////
    // Relationship
    ///////////////////////////////////////////////

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("모든 Parquet 파일을 merge한 경우")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("fail")
            .description("Parquet 파일의 merge를 처리할 수 없는 경우")
            .build();

    ///////////////////////////////////////////////
    // Property
    ///////////////////////////////////////////////

    public static final PropertyDescriptor PROP_HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration 설정 파일")
            .description("core-site.xml hdfs-site.xml 파일의 절대 경로를 comma separated 형식으로 지정합니다. Cloudera Manager > Clusters > [CLUSTER] > HDFS > Download Client Configution 메뉴를 통해서 다운로드할 수 있습니다." +
                    "NiFi 노드가 HDFS Gateway로 설정이 되어 있다면 /etc에 해당 파일이 있으므로 경로를 지정하도록 합니다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
            .name("원본 Parquet 파일 유지")
            .displayName("원본 Parquet 파일 유지")
            .description("다음 Processor로 전송 후에 원본 Parquet 파일을 삭제합니다.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor PROP_HDFS_SOURCE_PATH = new PropertyDescriptor.Builder()
            .name("Merge할 Parquet 파일이 있는 HDFS 디렉토리")
            .displayName("Merge할 Parquet 파일이 있는 HDFS 디렉토리")
            .description("Merge할 Parquet 파일을 HDFS에서 읽을때 사용하는 디렉토리입니다. 이 디렉토리의 *.parquet 파일을 모두 로딩합니다.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_HDFS_TARGET_DIR = new PropertyDescriptor.Builder()
            .name("Merge한 Parquet 파일을 저장할 HDFS 디렉토리")
            .displayName("Merge한 Parquet 파일을 저장할 HDFS 디렉토리")
            .description("Merge한 Parquet 파일을 HDFS에 저장할때 사용하는 디렉토리명입니다.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_HDFS_TARGET_FILENAME = new PropertyDescriptor.Builder()
            .name("Merge한 Parquet 파일의 파일명")
            .displayName("Merge한 Parquet 파일의 파일명")
            .description("Merge한 Parquet 파일을 HDFS에 저장할때 사용하는 파일명입니다.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    ///////////////////////////////////////////////////
    // onScheduled시 설정하는 값
    // 이 값들은 FlowFile Attribute에서 resolve할 수 없다.
    ///////////////////////////////////////////////////

    private final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    private static final HdfsResources EMPTY_HDFS_RESOURCES = new HdfsResources(null, null, null, null);

    ///////////////////////////////////////////////
    // Method
    ///////////////////////////////////////////////

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_HADOOP_CONFIGURATION_RESOURCES);
        properties.add(PROP_HDFS_SOURCE_PATH);
        properties.add(PROP_HDFS_TARGET_DIR);
        properties.add(PROP_HDFS_TARGET_FILENAME);
        properties.add(PROP_KEEP_SOURCE_FILE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    /**
     * Processor를 시작하면 HDFS에 접근하기 위한 core-site.xml hdfs-site.xml 파일을 미리 로딩해야 한다.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        try {
            HdfsResources resources = hdfsResources.get();

            // WARNING : NiFi에서 Run Once를 하면 아래 로직은 NPE가 발생하므로 Run을 하도록 한다.
            if (resources.getConfiguration() == null) {
                resources = resetHDFSResources(getConfigLocations(context), context);
                hdfsResources.set(resources);
            }
        } catch (Exception ex) {
            getLogger().error("HDFS Configuration 에러 - {}", new Object[]{ex});
            hdfsResources.set(EMPTY_HDFS_RESOURCES);
            // TODO : 필요한 경우에만 throw ex 한다.
            // TODO : throw하면 Run Once가 동작하지 않는다.
            throw ex;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        ComponentLog logger = this.getLogger();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // FlowFile Attribute에서 디렉토리 및 파일명을 확인합니다.
        String hdfsSourcePath = context.getProperty(PROP_HDFS_SOURCE_PATH).evaluateAttributeExpressions(flowFile).getValue();
        String hdfsTargetDir = context.getProperty(PROP_HDFS_TARGET_DIR).evaluateAttributeExpressions(flowFile).getValue();
        String hdfsTargetFilename = context.getProperty(PROP_HDFS_TARGET_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        boolean keepSourceFile = context.getProperty(PROP_KEEP_SOURCE_FILE).asBoolean();

        List<String> filesToDelete = new ArrayList();

        try {
            // HDFS에서 Parquet 파일을 로딩하여 Merge합니다.
            FileSystem fs = hdfsResources.get().getFileSystem();
            ParquetUtils.merge(fs, hdfsSourcePath, hdfsTargetDir, hdfsTargetFilename, logger, filesToDelete);

            // FlowFile을 전송하고 원본 파일 삭제 옵션이 ON인 경우 원본 파일을 삭제합니다.
            Map<String, String> newAttributes = new HashMap<>(flowFile.getAttributes());
            newAttributes.put("merge.hdfs.source.dir", hdfsSourcePath);
            newAttributes.put("merge.hdfs.source.files.count", String.valueOf(filesToDelete.size()));
            newAttributes.put("merge.hdfs.target.dir", hdfsTargetDir);
            newAttributes.put("merge.target.filename", hdfsTargetFilename);
            newAttributes.put("merge.target.qualifiedPath", String.format("%s/%s", hdfsTargetDir, hdfsTargetFilename));
            FlowFile f = session.putAllAttributes(flowFile, newAttributes);
            session.transfer(f, REL_SUCCESS);
            session.commitAsync(() -> {
                if (!keepSourceFile) {
                    logger.info("HDFS의 원본 Parquet 파일을 삭제하는 옵션이 ON입니다.");
                    ParquetUtils.deleteAll(logger, filesToDelete, hdfsResources.get().getFileSystem());
                }
            });
        } catch (Exception e) {
            logger.warn("Parquet 파일을 Merge할 수 없습니다. 원인: {}", e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * 이 Processor를 중지하면 core-site.xml hdfs-site.xml 파일의 리소스 정보를 초기화합니다.
     */
    @OnStopped
    public final void onStopped() {
        getLogger().info("MergeParquet Processor를 중지합니다.");
        final HdfsResources resources = hdfsResources.get();
        if (resources != null) {
            // 파일 시스템 종료를 시도합니다.
            final FileSystem fileSystem = resources.getFileSystem();
            try {
                interruptStatisticsThread(fileSystem);
            } catch (Exception e) {
                getLogger().warn("FileSystem 통계 쓰레드를 중지할 수 없습니다. 원인: " + e.getMessage());
                getLogger().debug("", e);
            } finally {
                if (fileSystem != null) {
                    try {
                        fileSystem.close();
                    } catch (IOException e) {
                        getLogger().warn("FileSystem을 종료할 수 없습니다. 원인: " + e.getMessage(), e);
                    }
                }
            }
        }

        // 리소스를 종료하고 클린업합니다.
        hdfsResources.set(EMPTY_HDFS_RESOURCES);
    }

    ///////////////////////////////////////////////
    // Utility
    ///////////////////////////////////////////////

    /**
     * Hadoop Configuration과 리소스를 종료합니다.
     */
    HdfsResources resetHDFSResources(final List<String> resourceLocations, final ProcessContext context) throws IOException {
        Configuration config = new ExtendedConfiguration(getLogger());
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        getConfigurationFromResources(config, resourceLocations);

        // HDFS 커넥션의 timeout을 확인합니다. 기본 FileSystem의 timeout은 하드코딩 되어 있으며 15분입니다.
        checkHdfsUriForTimeout(config);

        // Configuration과 FileSystem의 캐슁 옵션을 disabled합니다. 재시작 전까지 processor를 재설정할 수 없습니다.
        String disableCacheName = String.format("fs.%s.impl.disable.cache", FileSystem.getDefaultUri(config).getScheme());
        config.set(disableCacheName, "true");

        FileSystem fs = FileSystem.get(config);

        final Path workingDir = fs.getWorkingDirectory();
        getLogger().info("HDFS를 초기화할 수 없습니다 --> working dir: {} default block size: {} default replication: {} config: {}", new Object[]{workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config.toString()});

        return new HdfsResources(config, fs, null, null);
    }

    /**
     * Namenode의 포트가 개방되어 있는지 체크합니다.
     *
     * @param config Hadoop Configuration
     * @throws IOException 소켓 커넥션 실패시
     */
    protected void checkHdfsUriForTimeout(Configuration config) throws IOException {
        URI hdfsUri = FileSystem.getDefaultUri(config);
        String address = hdfsUri.getAuthority();
        int port = hdfsUri.getPort();
        if (address == null || address.isEmpty() || port < 0) {
            return;
        }

        // Namenode에 직접 소켓 케녁션을 timeout 1초로 설정하여 접근해본다.
        InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        Socket socket = null;
        try {
            socket = socketFactory.createSocket();
            NetUtils.connect(socket, namenode, 1000); // 1초 timeout
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    private static Configuration getConfigurationFromResources(final Configuration config, final List<String> locations) throws IOException {
        boolean foundResources = !locations.isEmpty();

        if (foundResources) {
            for (String resource : locations) {
                config.addResource(new Path(resource.trim()));
            }
        } else {
            String configStr = config.toString();
            for (String resource : configStr.substring(configStr.indexOf(":") + 1).split(",")) {
                if (!resource.contains("default") && config.getResource(resource.trim()) != null) {
                    foundResources = true;
                    break;
                }
            }
        }

        if (!foundResources) {
            throw new IOException("Classpath에서 " + PROP_HADOOP_CONFIGURATION_RESOURCES.getName() + "을 찾을 수 없습니다.");
        }
        return config;
    }

    private void interruptStatisticsThread(final FileSystem fileSystem) throws NoSuchFieldException, IllegalAccessException {
        final Field statsField = FileSystem.class.getDeclaredField("statistics");
        statsField.setAccessible(true);

        final Object statsObj = statsField.get(fileSystem);
        if (statsObj instanceof FileSystem.Statistics) {
            final FileSystem.Statistics statistics = (FileSystem.Statistics) statsObj;

            final Field statsThreadField = statistics.getClass().getDeclaredField("STATS_DATA_CLEANER");
            statsThreadField.setAccessible(true);

            final Object statsThreadObj = statsThreadField.get(statistics);
            if (statsThreadObj instanceof Thread) {
                final Thread statsThread = (Thread) statsThreadObj;
                try {
                    statsThread.interrupt();
                } catch (Exception e) {
                    getLogger().warn("쓰레드를 인터럽트할 수 없습니다. 원인 : " + e.getMessage(), e);
                }
            }
        }
    }

    protected List<String> getConfigLocations(PropertyContext context) {
        final ResourceReferences configResources = context.getProperty(PROP_HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().asResources();
        return configResources.asLocations();
    }
}