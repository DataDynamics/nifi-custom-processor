package io.datadynamics.nifi.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.security.krb.KerberosUser;

public class HdfsResources {
    private final Configuration configuration;
    private final FileSystem fileSystem;

    public HdfsResources(Configuration configuration, FileSystem fileSystem) {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
