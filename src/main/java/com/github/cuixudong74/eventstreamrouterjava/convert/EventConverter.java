package com.github.cuixudong74.eventstreamrouterjava.convert;

import com.github.cuixudong74.eventstreamrouterjava.core.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.core.ConversionOutcome;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 确定性版本转换：转换规则按版本维护，同一输入无论何时转换结果一致。
 * 无法转换的字段以 {@link ConversionIssue} 形式区分上报，不静默吞掉。
 * 未被任何映射覆盖的字段原样透传（确定行为）。
 */
@Component
public class EventConverter {

    private final ConversionRegistry registry;

    public EventConverter(ConversionRegistry registry) {
        this.registry = registry;
    }

    public ConversionOutcome convert(Map<String, Object> sourcePayload, int fromVersion, int targetVersion) {
        var chainOpt = registry.resolveChain(fromVersion, targetVersion);
        if (chainOpt.isEmpty()) {
            return ConversionOutcome.failed(List.of(new ConversionIssue(
                    "<schema>", ConversionIssue.NO_RULE,
                    "no conversion path from version " + fromVersion + " to " + targetVersion)));
        }
        Map<String, Object> current = new LinkedHashMap<>(sourcePayload);
        List<ConversionIssue> issues = new ArrayList<>();
        for (VersionConversion hop : chainOpt.get()) {
            current = applyHop(current, hop, issues);
        }
        return issues.isEmpty()
                ? ConversionOutcome.converted(current)
                : ConversionOutcome.partial(current, issues);
    }

    private Map<String, Object> applyHop(Map<String, Object> input, VersionConversion hop,
                                         List<ConversionIssue> issues) {
        Map<String, Object> out = new LinkedHashMap<>(input);
        for (FieldMapping m : hop.mappings()) {
            switch (m.transform()) {
                case COPY, RENAME -> {
                    if (!input.containsKey(m.sourceField())) {
                        issues.add(new ConversionIssue(m.targetField(), ConversionIssue.MISSING_SOURCE,
                                "source field '" + m.sourceField() + "' missing at version " + hop.fromVersion()));
                        out.remove(m.targetField());
                    } else {
                        out.put(m.targetField(), input.get(m.sourceField()));
                        if (m.transform() == FieldMapping.Transform.RENAME
                                && !m.sourceField().equals(m.targetField())) {
                            out.remove(m.sourceField());
                        }
                    }
                }
                case INT_TO_STRING -> {
                    Object v = input.get(m.sourceField());
                    if (v instanceof Number n) {
                        out.put(m.targetField(), String.valueOf(n.longValue()));
                    } else {
                        issues.add(new ConversionIssue(m.targetField(), ConversionIssue.INCOMPATIBLE_VALUE,
                                "field '" + m.sourceField() + "' is not a number: " + v));
                        out.remove(m.targetField());
                    }
                }
                case STRING_TO_INT -> {
                    Object v = input.get(m.sourceField());
                    if (v instanceof String s) {
                        try {
                            out.put(m.targetField(), Long.parseLong(s.trim()));
                        } catch (NumberFormatException e) {
                            issues.add(new ConversionIssue(m.targetField(), ConversionIssue.INCOMPATIBLE_VALUE,
                                    "field '" + m.sourceField() + "' is not a numeric string: '" + s + "'"));
                            out.remove(m.targetField());
                        }
                    } else {
                        issues.add(new ConversionIssue(m.targetField(), ConversionIssue.INCOMPATIBLE_VALUE,
                                "field '" + m.sourceField() + "' is not a string: " + v));
                        out.remove(m.targetField());
                    }
                }
                case CONSTANT -> out.put(m.targetField(), m.constantValue());
                case DROP -> out.remove(m.targetField());
            }
        }
        return out;
    }
}
