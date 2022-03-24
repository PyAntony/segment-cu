package com.charter.pauselive.scu.annot;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class Immutables {
    @Target({ElementType.PACKAGE, ElementType.TYPE})
    @Retention(RetentionPolicy.CLASS)
    @JsonDeserialize
    @Value.Style(
        // Detect names starting with underscore
        typeAbstract = "ABC*",
        // No prefix or suffix for generated immutable type
        typeImmutable = "*",
        // generated classes are PUBLIC (even if abstract class/interface is not)
        visibility = Value.Style.ImplementationVisibility.PUBLIC,
        // to replace attribute repr in toString
        redactedMask = "...",
        // will generate the static .of() builder with all parameters.
        allParameters = true
    )
    public @interface ImmutableStyle {}
}
