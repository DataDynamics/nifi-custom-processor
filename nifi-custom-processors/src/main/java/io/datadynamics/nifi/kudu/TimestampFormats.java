package io.datadynamics.nifi.kudu;

import java.io.Serializable;
import java.util.List;

public class TimestampFormats implements Serializable {

    List<TimestampFormat> formats;

    public List<TimestampFormat> getFormats() {
        return formats;
    }

    public void setFormats(List<TimestampFormat> formats) {
        this.formats = formats;
    }

}
