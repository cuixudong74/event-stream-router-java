package com.github.cuixudong74.eventstreamrouterjava.schema;

import com.github.cuixudong74.eventstreamrouterjava.core.FieldError;
import com.github.cuixudong74.eventstreamrouterjava.core.ValidationResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 按注册的结构版本校验事件 payload，逐字段报告错误。
 * 校验不通过即拒绝，绝不静默丢弃或按缺省值补全放行。
 */
@Component
public class SchemaValidator {

    public ValidationResult validate(EventSchema schema, Map<String, Object> payload) {
        List<FieldError> errors = new ArrayList<>();
        if (payload == null) {
            errors.add(new FieldError("<root>", "PAYLOAD_MISSING", "payload is required"));
            return ValidationResult.failed(errors);
        }
        Set<String> known = schema.fields().stream().map(SchemaField::name).collect(Collectors.toSet());

        for (SchemaField field : schema.fields()) {
            boolean present = payload.containsKey(field.name()) && payload.get(field.name()) != null;
            if (!present) {
                if (field.required()) {
                    errors.add(new FieldError(field.name(), "REQUIRED_MISSING",
                            "required field '" + field.name() + "' is missing or null"));
                }
                continue;
            }
            Object value = payload.get(field.name());
            if (!typeMatches(field.type(), value)) {
                errors.add(new FieldError(field.name(), "TYPE_MISMATCH",
                        "field '" + field.name() + "' expects " + field.type()
                                + " but got " + value.getClass().getSimpleName()));
            }
        }

        if (!schema.allowExtraFields()) {
            for (String key : payload.keySet()) {
                if (!known.contains(key)) {
                    errors.add(new FieldError(key, "UNKNOWN_FIELD",
                            "field '" + key + "' is not declared in schema version " + schema.version()));
                }
            }
        }

        return errors.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(errors);
    }

    private boolean typeMatches(FieldType type, Object value) {
        return switch (type) {
            case STRING -> value instanceof String;
            case NUMBER -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
            case OBJECT -> value instanceof Map;
            case ARRAY -> value instanceof List;
        };
    }
}
