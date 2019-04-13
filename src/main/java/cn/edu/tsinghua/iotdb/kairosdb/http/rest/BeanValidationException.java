package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;

/**
 * Thrown when bean validation has errors.
 */
public class BeanValidationException extends IOException
{
	private ImmutableSet<ConstraintViolation<Object>> violations;
	private String context;

	public BeanValidationException(Set<ConstraintViolation<Object>> violations)
	{
		this(violations, null);
	}

	public BeanValidationException(ConstraintViolation<Object> violation, String context)
	{
		this(Collections.singleton(violation), context);
	}

	public BeanValidationException(Set<ConstraintViolation<Object>> violations, String context)
	{
		super(messagesFor(violations, context).toString());
		this.context = context;
		this.violations = ImmutableSet.copyOf(violations);
	}

	/**
	 * Returns the bean validation error messages.
	 *
	 * @return validation error messages
	 */
	public List<String> getErrorMessages()
	{
		return messagesFor(violations, context);
	}

	/**
	 * Returns the set of bean validation violations.
	 *
	 * @return set of bean validation violations
	 */
	public Set<ConstraintViolation<Object>> getViolations()
	{
		return violations;
	}

	private static List<String> messagesFor(Set<ConstraintViolation<Object>> violations, String context)
	{
		ImmutableList.Builder<String> messages = new ImmutableList.Builder<String>();
		for (ConstraintViolation<?> violation : violations) {
			if (context != null && !context.isEmpty() )
				messages.add(context + "." + violation.getPropertyPath().toString() + " " + violation.getMessage());
			else
				messages.add(violation.getPropertyPath().toString() + " " + violation.getMessage());
		}

		return messages.build();
	}
}