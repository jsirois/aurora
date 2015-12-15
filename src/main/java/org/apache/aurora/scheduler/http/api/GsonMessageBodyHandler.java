/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import org.apache.aurora.thrift.ThriftEntity;
import org.apache.aurora.thrift.ThriftEntity.ThriftFields;
import org.apache.aurora.thrift.ThriftEntity.ThriftStruct;
import org.apache.aurora.thrift.ThriftEntity.ThriftUnion;
import org.apache.thrift.TUnion;

/**
 * A message body reader/writer that uses gson to translate JSON to and from java objects produced
 * by the thrift compiler.
 * <p>
 * This is used since jackson doesn't provide target type information to custom deserializer
 * implementations, so it is apparently not possible to implement a generic deserializer for
 * sublasses of {@link TUnion}.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GsonMessageBodyHandler
    implements MessageBodyReader<Object>, MessageBodyWriter<Object> {

  @Override
  public Object readFrom(
      Class<Object> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream) throws IOException {

    // For some reason try-with-resources syntax trips a findbugs error here.
    InputStreamReader streamReader = null;
    try {
      streamReader = new InputStreamReader(entityStream, StandardCharsets.UTF_8);
      Type jsonType;
      if (type.equals(genericType)) {
        jsonType = type;
      } else {
        jsonType = genericType;
      }
      return GSON.fromJson(streamReader, jsonType);
    } finally {
      if (streamReader != null) {
        streamReader.close();
      }
    }
  }

  @Override
  public void writeTo(
      Object o,
      Class<?> type,
      Type genericType, Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders,
      OutputStream entityStream) throws IOException, WebApplicationException {

    try (OutputStreamWriter writer = new OutputStreamWriter(entityStream, StandardCharsets.UTF_8)) {
      Type jsonType;
      if (type.equals(genericType)) {
        jsonType = type;
      } else {
        jsonType = genericType;
      }
      GSON.toJson(o, jsonType, writer);
    }
  }

  @Override
  public boolean isReadable(
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return true;
  }

  @Override
  public boolean isWriteable(
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return true;
  }

  @Override
  public long getSize(
      Object o,
      Class<?> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType) {

    return -1;
  }

  public static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(ImmutableList.class, new JsonDeserializer<ImmutableList<?>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ImmutableList<?> deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {

          Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
          Type typeArgument = typeArguments[0];

          @SuppressWarnings("rawtypes")
          ImmutableList.Builder builder = ImmutableList.builder();
          for (JsonElement element : json.getAsJsonArray()) {
            Object value = context.deserialize(element, typeArgument);
            builder.add(value);
          }
          return builder.build();
        }
      })
      .registerTypeAdapter(ImmutableSet.class, new JsonDeserializer<ImmutableSet<?>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ImmutableSet<?> deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {

          Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
          Type typeArgument = typeArguments[0];

          @SuppressWarnings("rawtypes")
          ImmutableSet.Builder builder = ImmutableSet.builder();
          for (JsonElement element : json.getAsJsonArray()) {
            Object value = context.deserialize(element, typeArgument);
            builder.add(value);
          }
          return builder.build();
        }
      })
      .registerTypeAdapter(ImmutableMap.class, new JsonDeserializer<ImmutableMap<?, ?>>() {
        @Override
        @SuppressWarnings("unchecked")
        public ImmutableMap<?, ?> deserialize(
            JsonElement json,
            Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {

          Type[] typeArguments = ((ParameterizedType) typeOfT).getActualTypeArguments();
          Type valueTypeArg = typeArguments[1];

          @SuppressWarnings("rawtypes")
          ImmutableMap.Builder builder = ImmutableMap.builder();
          for (Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
            Object value = context.deserialize(entry.getValue(), valueTypeArg);
            builder.put(entry.getKey(), value);
          }
          return builder.build();
        }
      })
      .registerTypeHierarchyAdapter(ThriftStruct.class,
          new JsonSerializer<ThriftStruct<ThriftFields>>() {
            @Override
            public JsonElement serialize(
                ThriftStruct<ThriftFields> src,
                Type typeOfSrc,
                JsonSerializationContext context) {

              ImmutableMap.Builder<String, Object> data = ImmutableMap.builder();
              for (ThriftFields field : src.getFields()) {
                if (src.isSet(field)) {
                  data.put(field.getFieldName(), src.getFieldValue(field));
                }
              }
              return context.serialize(data.build());
            }
          })
      .registerTypeHierarchyAdapter(ThriftStruct.class,
          new JsonDeserializer<ThriftStruct<ThriftFields>>() {
            @Override
            public ThriftStruct<ThriftFields> deserialize(
                JsonElement json,
                Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {

              JsonObject jsonObject = json.getAsJsonObject();
              if (typeOfT instanceof Class) {
                @SuppressWarnings("unchecked")
                Class<ThriftStruct<ThriftFields>> clazz =
                    (Class<ThriftStruct<ThriftFields>>) typeOfT;

                ThriftStruct.Builder<ThriftFields, ThriftStruct<ThriftFields>> builder =
                    ThriftStruct.builder(clazz);
                for (ThriftFields field : ThriftEntity.fields(clazz)) {
                  JsonElement element = jsonObject.get(field.getFieldName());
                  Object value = context.deserialize(element, field.getFieldType());
                  builder.set(field, value);
                }
                return builder.build();
              } else {
                throw new RuntimeException("Unable to deserialize " + typeOfT);
              }
            }
          })
      .registerTypeHierarchyAdapter(ThriftUnion.class,
          new JsonSerializer<ThriftUnion<ThriftFields>>() {
            @Override
            public JsonElement serialize(
                ThriftUnion<ThriftFields> src,
                Type typeOfSrc,
                JsonSerializationContext context) {

              return context.serialize(
                  ImmutableMap.of(src.getSetField().getFieldName(), src.getFieldValue()));
            }
          })
      .registerTypeHierarchyAdapter(ThriftUnion.class,
          new JsonDeserializer<ThriftUnion<ThriftFields>>() {
            @Override
            public ThriftUnion<ThriftFields> deserialize(
                JsonElement json,
                Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {

              JsonObject jsonObject = json.getAsJsonObject();
              if (jsonObject.entrySet().size() != 1) {
                throw new JsonParseException(
                    typeOfT.getClass().getName() + " must have exactly one element");
              }

              if (typeOfT instanceof Class) {
                @SuppressWarnings("unchecked")
                Class<ThriftUnion<ThriftFields>> clazz = (Class<ThriftUnion<ThriftFields>>) typeOfT;
                Entry<String, JsonElement> item = Iterables.getOnlyElement(jsonObject.entrySet());

                for (ThriftFields field : ThriftEntity.fields(clazz)) {
                  if (field.getFieldName().equals(item.getKey())) {
                    Object value = context.deserialize(item.getValue(), field.getFieldType());
                    return ThriftUnion.create(clazz, field, value);
                  }
                }
                throw new RuntimeException("Failed to deserialize " + typeOfT);
              } else {
                throw new RuntimeException("Unable to deserialize " + typeOfT);
              }
            }
          })
      .create();
}
