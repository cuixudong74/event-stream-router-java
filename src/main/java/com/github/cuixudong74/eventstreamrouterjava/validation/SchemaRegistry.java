package com.github.cuixudong74.eventstreamrouterjava.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationError;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class SchemaRegistry {
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private final Map<String, EventSchema> schemas = Map.of(
            "1", new V1EventSchema(),
            "2", new V2EventSchema()
    );

    public Optional<EventSchema> find(String version) {
        return Optional.ofNullable(schemas.get(version));
    }

    private static final class V1EventSchema implements EventSchema {
        private static final Set<String> ALLOWED = Set.of("amount", "currency", "customerType", "occurredAt");

        @Override
        public String version() {
            return "1";
        }

        @Override
        public void validatePayload(JsonNode payload, List<ValidationError> errors) {
            requireNumber(payload, "amount", errors);
            requireCurrency(payload, "currency", errors);
            validateCustomerType(payload, errors);
            validateOptionalInstant(payload, "occurredAt", errors);
            rejectUnknownFields(payload, ALLOWED, errors);
        }
    }

    private static final class V2EventSchema implements EventSchema {
        private static final Set<String> ALLOWED = Set.of("amount", "customerType", "occurredAt");

        @Override
        public String version() {
            return "2";
        }

        @Override
        public void validatePayload(JsonNode payload, List<ValidationError> errors) {
            JsonNode amount = payload.path("amount");
            if (amount.isMissingNode() || amount.isNull()) {
                errors.add(new ValidationError("payload.amount", "REQUIRED", "amount is required"));
            } else if (!amount.isObject()) {
                errors.add(new ValidationError("payload.amount", "TYPE", "amount must be an object"));
            } else {
                requireNumber(amount, "value", "payload.amount.value", errors);
                requireCurrency(amount, "currency", "payload.amount.currency", errors);
                rejectUnknownFields(amount, Set.of("value", "currency"), "payload.amount", errors);
            }
            validateCustomerType(payload, errors);
            requireInstant(payload, "occurredAt", errors);
            rejectUnknownFields(payload, ALLOWED, errors);
        }
    }

    private static void requireNumber(JsonNode node, String field, List<ValidationError> errors) {
        requireNumber(node, field, "payload." + field, errors);
    }

    private static void requireNumber(JsonNode node, String field, String path, List<ValidationError> errors) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            errors.add(new ValidationError(path, "REQUIRED", field + " is required"));
        } else if (!value.isNumber()) {
            errors.add(new ValidationError(path, "TYPE", field + " must be a number"));
        } else if (value.doubleValue() < 0) {
            errors.add(new ValidationError(path, "RANGE", field + " must not be negative"));
        }
    }

    private static void requireCurrency(JsonNode node, String field, List<ValidationError> errors) {
        requireCurrency(node, field, "payload." + field, errors);
    }

    private static void requireCurrency(JsonNode node, String field, String path, List<ValidationError> errors) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            errors.add(new ValidationError(path, "REQUIRED", field + " is required"));
        } else if (!value.isTextual()) {
            errors.add(new ValidationError(path, "TYPE", field + " must be a string"));
        } else if (!CURRENCY.matcher(value.asText()).matches()) {
            errors.add(new ValidationError(path, "FORMAT", field + " must be three uppercase letters"));
        }
    }

    private static void validateCustomerType(JsonNode payload, List<ValidationError> errors) {
        JsonNode value = payload.path("customerType");
        if (value.isMissingNode() || value.isNull()) {
            return;
        }
        if (!value.isTextual()) {
            errors.add(new ValidationError("payload.customerType", "TYPE", "customerType must be a string"));
        } else if (!Set.of("RETAIL", "BUSINESS", "VIP").contains(value.asText())) {
            errors.add(new ValidationError("payload.customerType", "ENUM", "customerType is unsupported"));
        }
    }

    private static void validateOptionalInstant(JsonNode payload, String field, List<ValidationError> errors) {
        JsonNode value = payload.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return;
        }
        requireInstant(payload, field, errors);
    }

    private static void requireInstant(JsonNode payload, String field, List<ValidationError> errors) {
        requireInstant(payload, field, "payload." + field, errors);
    }

    private static void requireInstant(JsonNode payload, String field, String path, List<ValidationError> errors) {
        JsonNode value = payload.path(field);
        if (value.isMissingNode() || value.isNull()) {
            errors.add(new ValidationError(path, "REQUIRED", field + " is required"));
        } else if (!value.isTextual()) {
            errors.add(new ValidationError(path, "TYPE", field + " must be a timestamp string"));
        } else {
            try {
                Instant.parse(value.asText());
            } catch (RuntimeException ignored) {
                errors.add(new ValidationError(path, "FORMAT", field + " must be an ISO-8601 UTC timestamp"));
            }
        }
    }

    private static void rejectUnknownFields(JsonNode node, Set<String> allowed, List<ValidationError> errors) {
        rejectUnknownFields(node, allowed, "payload", errors);
    }

    private static void rejectUnknownFields(JsonNode node, Set<String> allowed, String prefix,
                                            List<ValidationError> errors) {
        if (!node.isObject()) {
            errors.add(new ValidationError(prefix, "TYPE", prefix + " must be an object"));
            return;
        }
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        for (String name : names) {
            if (!allowed.contains(name)) {
                errors.add(new ValidationError(prefix + "." + name, "UNKNOWN_FIELD", "field is not supported"));
            }
        }
    }
}
