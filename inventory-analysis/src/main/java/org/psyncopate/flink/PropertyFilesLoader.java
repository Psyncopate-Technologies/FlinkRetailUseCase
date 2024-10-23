package org.psyncopate.flink;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFilesLoader {
    public static Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = PropertyFilesLoader.class.getClassLoader()
                .getResourceAsStream(fileName)) {
            if (input == null) {
                throw new IOException("Unable to find"+fileName);
            }
            properties.load(input);
        }
        return properties;
    }
    
}