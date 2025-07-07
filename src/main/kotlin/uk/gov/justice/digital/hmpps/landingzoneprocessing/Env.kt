package uk.gov.justice.digital.hmpps.landingzoneprocessing

/**
 * For retrieving environment variables
 */
object Env {
    const val OUTPUT_BUCKET_ENV_KEY = "OUTPUT_BUCKET"
    const val SCHEMA_REGISTRY_BUCKET_ENV_KEY = "SCHEMA_REGISTRY_BUCKET"
    const val VIOLATIONS_BUCKET_ENV_KEY = "VIOLATIONS_BUCKET"
    const val VIOLATIONS_PATH_ENV_KEY = "VIOLATIONS_PATH"
    const val CHARSET_ENV_KEY = "CHARSET"
    const val NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY = "NUMBER_OF_HEADER_ROWS_TO_SKIP"
    const val LOG_CSV_ENV_KEY = "LOG_CSV"

    fun get(name: String): String = requireNotNull(System.getenv(name)) { "$name environment variable must not be null" }
}
