package org.swallow.util;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class XmlUtils {
    private static final XmlMapper XML_MAPPER = new XmlMapper();

    public static <T> T toBean(String xml, Class<T> cls) throws Exception {
        return XML_MAPPER.readValue(xml, cls);
    }
}
