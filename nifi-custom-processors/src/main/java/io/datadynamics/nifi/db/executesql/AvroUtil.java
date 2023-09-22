package io.datadynamics.nifi.db.executesql;


import org.apache.avro.file.CodecFactory;

/**
 * Avro Utils.
 */
public class AvroUtil {

    public static CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
            case BZIP2:
                return CodecFactory.bzip2Codec();
            case DEFLATE:
                return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case LZO:
                return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case SNAPPY:
                return CodecFactory.snappyCodec();
            case NONE:
            default:
                return CodecFactory.nullCodec();
        }
    }

    public static enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }

}