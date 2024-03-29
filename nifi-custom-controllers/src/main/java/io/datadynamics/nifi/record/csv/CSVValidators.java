package io.datadynamics.nifi.record.csv;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.HashSet;
import java.util.Set;

public class CSVValidators {
    public static final Validator UNESCAPED_SINGLE_CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

            if (input == null) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation("Input is null for this property")
                        .build();
            }

            String unescaped = org.apache.nifi.csv.CSVUtils.unescape(input);

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(unescaped)
                    .explanation("Only non-null single characters are supported")
                    .valid((unescaped.length() == 1 && unescaped.charAt(0) != 0)
                            || (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)))
                    .build();
        }
    };
    private static final Set<String> illegalChars = new HashSet<>();
    public static final Validator SINGLE_CHAR_VALIDATOR = createSingleCharValidator(false);

    public static final Validator EMPTY_OR_SINGLE_CHAR_VALIDATOR = createSingleCharValidator(true);

    static {
        illegalChars.add("\r");
        illegalChars.add("\n");
    }

    private static Validator createSingleCharValidator(final boolean canBeEmpty) {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                if (input == null) {
                    return new ValidationResult.Builder()
                            .input(input)
                            .subject(subject)
                            .valid(false)
                            .explanation("Input is null for this property")
                            .build();
                }

                if (input.isEmpty()) {
                    return new ValidationResult.Builder()
                            .input(input)
                            .subject(subject)
                            .valid(canBeEmpty)
                            .explanation("Value must be exactly 1 character but was 0 in length")
                            .build();
                }


                if (!context.isExpressionLanguageSupported(subject) || !context.isExpressionLanguagePresent(input)) {
                    final String unescaped = CSVUtils.unescape(input);
                    if (unescaped.length() != 1) {
                        return new ValidationResult.Builder()
                                .input(input)
                                .subject(subject)
                                .valid(false)
                                .explanation("Value must be exactly 1 character but was " + input.length() + " in length")
                                .build();
                    }

                    if (illegalChars.contains(unescaped)) {
                        return new ValidationResult.Builder()
                                .input(input)
                                .subject(subject)
                                .valid(false)
                                .explanation(input + " is not a valid character for this property")
                                .build();
                    }
                }

                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(true)
                        .build();
            }
        };
    }

}