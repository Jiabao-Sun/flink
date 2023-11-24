package org.apache.flink.architecture;

import org.apache.flink.architecture.common.ImportOptions;
import org.apache.flink.architecture.rules.BanJunit4Rules;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/** Architecture tests for test code. */
@AnalyzeClasses(
        packages = {
                "org.apache.flink.connector",
                "org.apache.flink.formats",
                "org.apache.flink.table.api",
                "org.apache.flink.table.planner",
                "org.apache.flink.table.test",
        },
        importOptions = {
            ImportOption.OnlyIncludeTests.class,
            ImportOptions.ExcludeScalaImportOption.class,
            ImportOptions.ExcludeShadedImportOption.class
        })
public class TestCodeArchitectureTest {

    @ArchTest public static final ArchTests BAN_JUNIT4_TESTS = ArchTests.in(BanJunit4Rules.class);
}
