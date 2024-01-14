package io.datadynamics.nifi.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.security.krb.KerberosUser;

public class HdfsResources {
    private final Configuration configuration;
    private final FileSystem fileSystem;
    private final UserGroupInformation userGroupInformation;
    private final KerberosUser kerberosUser;

    public HdfsResources(Configuration configuration, FileSystem fileSystem, UserGroupInformation userGroupInformation, KerberosUser kerberosUser) {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
        this.userGroupInformation = userGroupInformation;
        this.kerberosUser = kerberosUser;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    public KerberosUser getKerberosUser() {
        return kerberosUser;
    }
}
