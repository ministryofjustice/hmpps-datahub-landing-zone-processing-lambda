package uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers

object TestDataCsv {
    val csvNoHeader = """
            a,1,2,3.0
            b,2,3,4.1
            c,3,4,5.2
        """.trimIndent()

    val csvWithHeader = """
            id,no1,no2,no3
            a,1,2,3.0
            b,2,3,4.1
            c,3,4,5.2
        """.trimIndent()

    val csvWithMissingValues = """
            a,1,,3.0
            b,,3,4.1
            c,,,
        """.trimIndent()

    val csvDifferentNumberOfColumnsInDifferentRows = """
            a,1,2,3,4,5
            b,1,2,3,4
        """.trimIndent()

    val csvWithTrailingEmptyRows = """
            a,1,2,3.0
            b,2,3,4.1
            c,3,4,5.2
            ,,,
            ,,,
            ,,,
        """.trimIndent()

    val csvWithTrailingEmptyColumns = """
            a,1,2,3.0,,,,,
            b,2,3,4.1,,,,,
            c,3,4,5.2,,,,,
        """.trimIndent()

    val csvWithSpecialCharacters = """
            a,©
            b,ü
            c,ç
        """.trimIndent()
}