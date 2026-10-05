package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.core.ValidationResult;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.FieldType;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaField;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaValidator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 结构版本校验：不兼容必须拒绝并定位到具体字段，不得静默丢弃或补默认值。
 */
class SchemaValidationTest {

    private final SchemaValidator validator = new SchemaValidator();

    private EventSchema schemaV1() {
        return new EventSchema(1, List.of(
                new SchemaField("orderId", FieldType.STRING, true),
                new SchemaField("amount", FieldType.NUMBER, true),
                new SchemaField("note", FieldType.STRING, false)), false);
    }

    @Test
    void validPayloadPasses() {
        ValidationResult r = validator.validate(schemaV1(),
                Map.of("orderId", "o1", "amount", 100, "note", "x"));
        assertThat(r.valid()).isTrue();
    }

    @Test
    void missingRequiredFieldIsLocated() {
        ValidationResult r = validator.validate(schemaV1(), Map.of("orderId", "o1"));
        assertThat(r.valid()).isFalse();
        assertThat(r.errors()).anySatisfy(e -> {
            assertThat(e.field()).isEqualTo("amount");
            assertThat(e.code()).isEqualTo("REQUIRED_MISSING");
        });
    }

    @Test
    void typeMismatchIsLocated() {
        ValidationResult r = validator.validate(schemaV1(),
                Map.of("orderId", "o1", "amount", "not-a-number"));
        assertThat(r.valid()).isFalse();
        assertThat(r.errors()).anySatisfy(e -> {
            assertThat(e.field()).isEqualTo("amount");
            assertThat(e.code()).isEqualTo("TYPE_MISMATCH");
        });
    }

    @Test
    void unknownFieldRejectedWhenNotAllowed() {
        ValidationResult r = validator.validate(schemaV1(),
                Map.of("orderId", "o1", "amount", 1, "surprise", true));
        assertThat(r.valid()).isFalse();
        assertThat(r.errors()).anySatisfy(e -> {
            assertThat(e.field()).isEqualTo("surprise");
            assertThat(e.code()).isEqualTo("UNKNOWN_FIELD");
        });
    }

    @Test
    void nullPayloadRejected() {
        ValidationResult r = validator.validate(schemaV1(), null);
        assertThat(r.valid()).isFalse();
        assertThat(r.errors()).isNotEmpty();
    }
}
