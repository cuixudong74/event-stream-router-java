package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import com.github.cuixudong74.eventstreamrouterjava.convert.ConversionRegistry;
import com.github.cuixudong74.eventstreamrouterjava.convert.EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.convert.FieldMapping;
import com.github.cuixudong74.eventstreamrouterjava.convert.VersionConversion;
import com.github.cuixudong74.eventstreamrouterjava.core.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.core.ConversionOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 版本转换：确定性、按版本维护规则、无法转换字段可区分。
 */
class ConversionTest {

    @TempDir
    Path dir;

    private EventConverter converterWithRules() {
        RouterProperties props = new RouterProperties();
        props.setDataDir(dir.toString());
        ConversionRegistry registry = new ConversionRegistry(props);
        // v1 -> v2：amount(int) -> amountText(string)，orderId 改名 id
        registry.register(new VersionConversion(1, 2, List.of(
                new FieldMapping("id", "orderId", FieldMapping.Transform.RENAME, null),
                new FieldMapping("amountText", "amount", FieldMapping.Transform.INT_TO_STRING, null),
                new FieldMapping("amount", null, FieldMapping.Transform.DROP, null))));
        // v2 -> v3：增加常量字段 currency
        registry.register(new VersionConversion(2, 3, List.of(
                new FieldMapping("currency", null, FieldMapping.Transform.CONSTANT, "CNY"))));
        return new EventConverter(registry);
    }

    @Test
    void convertsAcrossVersionChain() {
        ConversionOutcome out = converterWithRules().convert(
                Map.of("orderId", "o1", "amount", 42), 1, 3);
        assertThat(out.status()).isEqualTo(ConversionOutcome.Status.CONVERTED);
        assertThat(out.payload()).containsEntry("id", "o1")
                .containsEntry("amountText", "42")
                .containsEntry("currency", "CNY")
                .doesNotContainKey("orderId")
                .doesNotContainKey("amount");
    }

    @Test
    void conversionIsDeterministic() {
        EventConverter c = converterWithRules();
        Map<String, Object> payload = Map.of("orderId", "o1", "amount", 42);
        ConversionOutcome a = c.convert(payload, 1, 3);
        ConversionOutcome b = c.convert(payload, 1, 3);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void incompatibleFieldProducesDistinguishableIssue() {
        ConversionOutcome out = converterWithRules().convert(
                Map.of("orderId", "o1", "amount", "not-a-number"), 1, 2);
        assertThat(out.status()).isEqualTo(ConversionOutcome.Status.PARTIAL);
        assertThat(out.issues()).anySatisfy(i -> {
            assertThat(i.field()).isEqualTo("amountText");
            assertThat(i.code()).isEqualTo(ConversionIssue.INCOMPATIBLE_VALUE);
        });
    }

    @Test
    void missingSourceProducesDistinguishableIssue() {
        ConversionOutcome out = converterWithRules().convert(Map.of("amount", 1), 1, 2);
        assertThat(out.status()).isEqualTo(ConversionOutcome.Status.PARTIAL);
        assertThat(out.issues()).anySatisfy(i -> {
            assertThat(i.field()).isEqualTo("id");
            assertThat(i.code()).isEqualTo(ConversionIssue.MISSING_SOURCE);
        });
    }

    @Test
    void noConversionPathFails() {
        RouterProperties props = new RouterProperties();
        props.setDataDir(dir.toString());
        EventConverter c = new EventConverter(new ConversionRegistry(props));
        ConversionOutcome out = c.convert(Map.of("x", 1), 7, 9);
        assertThat(out.status()).isEqualTo(ConversionOutcome.Status.FAILED);
        assertThat(out.issues()).anySatisfy(i -> assertThat(i.code()).isEqualTo(ConversionIssue.NO_RULE));
    }

    @Test
    void sameVersionIsIdentity() {
        ConversionOutcome out = converterWithRules().convert(Map.of("a", 1), 3, 3);
        assertThat(out.status()).isEqualTo(ConversionOutcome.Status.CONVERTED);
        assertThat(out.payload()).containsEntry("a", 1);
    }
}
