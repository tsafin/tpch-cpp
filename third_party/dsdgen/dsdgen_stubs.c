/*
 * dsdgen_stubs.c
 *
 * Placeholder for any stub implementations needed when embedding dsdgen
 * as a library.  Mirrors src/dbgen/dbgen_stubs.c for TPC-H dbgen.
 *
 * Currently empty: all required symbols are provided by the dsdgen sources
 * themselves.  The main() collision in driver.c is handled at compile time
 * via -Dmain=dsdgen_driver_main_ in CMakeLists.txt.
 */
