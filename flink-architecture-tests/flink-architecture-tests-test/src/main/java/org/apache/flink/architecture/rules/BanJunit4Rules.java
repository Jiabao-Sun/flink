package org.apache.flink.architecture.rules;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

/** Rules for modules already completed the migration of junit5. */
public class BanJunit4Rules {

    @ArchTest
    public static final ArchRule NO_JUNIT4_DEPENDENCIES_RULE =
            freeze(
                    noClasses()
                            .that()
                            .resideInAPackage("org.apache.flink..")
                            .should()
                            .dependOnClassesThat()
                            .resideInAnyPackage("junit", "org.junit")
                            .as("Junit4 is forbidden, please use Junit5 instead"));
}
