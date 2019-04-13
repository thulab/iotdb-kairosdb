package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.BeanValidationException;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.metadata.ConstraintDescriptor;
import org.apache.bval.jsr303.ApacheValidationProvider;

public class QueryParser {

  private Gson gson;
  private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();


  @Inject
  public QueryParser() {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Query.class, new GroupByDeserializer());
    gson = gsonBuilder.create();
  }

  public Query parseQueryMetric(String json) throws QueryException, BeanValidationException {
    JsonParser parser = new JsonParser();
    JsonObject obj = parser.parse(json).getAsJsonObject();
    return parseQueryMetric(obj);
  }

  private Query parseQueryMetric(JsonObject obj) throws QueryException, BeanValidationException {
    return parseQueryMetric(obj, "");
  }

  private Query parseQueryMetric(JsonObject obj, String contextPrefix) throws QueryException, BeanValidationException {
    Query query;
    try {
      query = gson.fromJson(obj, Query.class);
      Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(query);
      if (!violations.isEmpty()) {
        throw new BeanValidationException(violations, null);
      }
      List<QueryMetric> metrics = query.getQueryMetrics();
      if (metrics == null) {
        throw new BeanValidationException(new SimpleConstraintViolation("metric[]", "must have a size of at least 1"), contextPrefix + "query");
      }
    } catch (ContextualJsonSyntaxException e) {
      throw new BeanValidationException(new SimpleConstraintViolation(e.getContext(), e.getMessage()), "query");
    }

    return query;
  }

  private static class ContextualJsonSyntaxException extends RuntimeException {
    private String context;

    private ContextualJsonSyntaxException(String context, String msg)
    {
      super(msg);
      this.context = context;
    }

    private String getContext()
    {
      return context;
    }
  }

  public static class SimpleConstraintViolation implements ConstraintViolation<Object> {
    private String message;
    private String context;

    public SimpleConstraintViolation(String context, String message)
    {
      this.message = message;
      this.context = context;
    }

    @Override
    public String getMessage()
    {
      return message;
    }

    @Override
    public String getMessageTemplate()
    {
      return null;
    }

    @Override
    public Object getRootBean()
    {
      return null;
    }

    @Override
    public Class<Object> getRootBeanClass()
    {
      return null;
    }

    @Override
    public Object getLeafBean()
    {
      return null;
    }

    @Override
    public Object[] getExecutableParameters() {
      return new Object[0];
    }

    @Override
    public Object getExecutableReturnValue() {
      return null;
    }

    @Override
    public Path getPropertyPath()
    {
      return new SimplePath(context);
    }

    @Override
    public Object getInvalidValue()
    {
      return null;
    }

    @Override
    public ConstraintDescriptor<?> getConstraintDescriptor()
    {
      return null;
    }

    @Override
    public <U> U unwrap(Class<U> aClass) {
      return null;
    }
  }

  private static class SimplePath implements Path {
    private String context;

    private SimplePath(String context) {
      this.context = context;
    }

    @Override
    public Iterator<Node> iterator()
    {
      return null;
    }

    @Override
    public String toString()
    {
      return context;
    }
  }

}
