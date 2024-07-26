package com.lin.common.util;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;

/**
 * @author linzj
 */
public class JsonUtil {
    public static final String CODE = "code";
    public static final String STATUS = "status";
    public static final String DATA = "data";
    public static final String MESSAGE = "message";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonFactory jsonFactory = mapper.getFactory();
    static {
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        // 当反序列化出现未定义字段时，不出现错误
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
                .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        // dubbo泛化调用去除class字段
        mapper.addMixIn(Object.class, ExcludeFilter.class);
        mapper.setFilterProvider(new SimpleFilterProvider()
                .addFilter("excludeFilter", SimpleBeanPropertyFilter.serializeAllExcept("class")));
    }

    public static String toJsonString(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("object format to json error:" + obj, e);
        }
    }

    public static void output2Writer(Writer out, Object value) {
        try {
            mapper.writeValue(out, value);
        }catch (Exception e) {
            throw new RuntimeException("output to writer error:"+value, e);
        }
    }

    /**
     * jsonNode反转为bean的时候，bean必须有缺省的构造函数，不然json直接用clz.getConstructor时候，无法找到默认构造函数
     * @param body
     * @param clz
     * @return
     * @param <T>
     */
    public static <T> T parse(JsonNode body, Class<T> clz) {
        try {
            return mapper.readValue(body.traverse(), clz);
        } catch (Exception e) {
            throw new RuntimeException("json node parse to object ["+clz+"] error:"+body,e);
        }
    }

    public static <T> T parse(String str, Class<T> clz) {
        try {
            return mapper.readValue(str == null ? "{}" : str, clz);
        }catch (Exception e) {
            throw new RuntimeException("json parse to object [" + clz + "] error:"+str, e);
        }
    }

    public static <T> T parse(Optional<String> json, Class<T> clz) {
        return json.map((str) -> parse(str, clz)).orElse(null);
    }

    public static <T> T parse(String str, TypeReference<T> tr) {
        try{
            return mapper.readValue(str, tr);
        } catch (Exception e) {
            throw new RuntimeException("json parse to object [" + tr +"] error:"+str, e);
        }
    }

    public static <T> T parse(JsonNode body, JavaType javaType) {
        try {
            return mapper.readValue(body.traverse(), javaType);
        } catch (Exception e) {
            throw new RuntimeException("json parse to object [" + javaType + "] error:"+body, e);
        }
    }

    public static <T> T parse(String str, JavaType javaType) {
        try{
            return mapper.readValue(str, javaType);
        }catch (Exception e) {
            throw new RuntimeException("json parse to object [" + javaType + "] error:"+str, e);
        }
    }

    public static <T> List<T> parse2List(String json, Class<T> clz) {
        return parse(json, getCollectionType(List.class, clz));
    }

    public static JsonNode tree(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("json parse to object [tree] error"+json,e);
        }
    }

    public static String serializeAllExcept(Object obj, String... filterFields) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

            FilterProvider filters = new SimpleFilterProvider()
                    .addFilter(obj.getClass().getName(),
                            SimpleBeanPropertyFilter.serializeAllExcept(filterFields));
            mapper.setFilterProvider(filters)
                    .setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
                        @Override
                        public Object findFilterId(Annotated ac) {
                            return ac.getName();
                        }
                    });

            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("object format to json error:" + obj, e);
        }
    }

    @SuppressWarnings("serial")
    public static String filterOutAllExcept(Object obj, String... filterFields) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

            FilterProvider filters = new SimpleFilterProvider()
                    .addFilter(obj.getClass().getName(),
                            SimpleBeanPropertyFilter.filterOutAllExcept(filterFields));
            mapper.setFilterProvider(filters)
                    .setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
                        @Override
                        public Object findFilterId(Annotated ac) {
                            return ac.getName();
                        }
                    });

            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("object format to json error:" + obj, e);
        }
    }

    public static String parseOneField(String str, String fieldName) {
        try {
            JsonParser jsonParser = jsonFactory.createParser(str);
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                // get the current token
                String fieldname = jsonParser.getCurrentName();
                if (fieldName.equals(fieldname)) {
                    // move to next token
                    jsonParser.nextToken();
                    return jsonParser.getText();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("object format to json error:", e);
        }
        return null;
    }

    public static ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }

    public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return mapper.getTypeFactory().
                constructParametricType(collectionClass, elementClasses);
    }

    public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
        return mapper.convertValue(fromValue, toValueType);
    }

    @JsonFilter("excludeFilter")
    public static class ExcludeFilter{

    }
}
