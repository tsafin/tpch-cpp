#ifndef TPCH_LANCE_FFI_H
#define TPCH_LANCE_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque handle to a Lance writer.
 * Created by lance_writer_create() and destroyed by lance_writer_destroy().
 */
typedef struct LanceWriter LanceWriter;

/**
 * Create a new Lance writer for writing to the specified URI.
 *
 * @param uri Path to write to (e.g., "/tmp/dataset" or "s3://bucket/path")
 * @param arrow_schema_ptr Opaque pointer to Arrow C Data Interface ArrowSchema struct
 *        (Can be NULL; schema will be inferred from first batch)
 *
 * @return Opaque pointer to LanceWriter on success, NULL on error
 *
 * The returned pointer must be passed to lance_writer_write_batch(),
 * lance_writer_close(), and eventually lance_writer_destroy().
 */
LanceWriter* lance_writer_create(const char* uri, const void* arrow_schema_ptr);

/**
 * Write a batch of records to the Lance dataset.
 *
 * Imports Arrow C Data Interface structures and accumulates batches for
 * efficient Lance dataset writing. The batch is not actually written to disk
 * until lance_writer_close() is called, at which point all accumulated batches
 * are written as a single Lance dataset.
 *
 * @param writer Pointer to LanceWriter from lance_writer_create()
 * @param arrow_array_ptr Opaque pointer to Arrow C Data Interface ArrowArray struct
 * @param arrow_schema_ptr Opaque pointer to Arrow C Data Interface ArrowSchema struct
 *
 * @return 0 on success, non-zero error code on failure:
 *         1 = writer_ptr is null
 *         2 = Writer is already closed
 *         3 = arrow_array_ptr or arrow_schema_ptr is null
 *         4 = Failed to import Arrow C Data Interface
 *         7 = Panic in lance_writer_write_batch
 *         (Other non-zero values are reserved for future use.)
 *
 * The ArrowArray and ArrowSchema pointers must point to valid C Data Interface
 * structs. Memory ownership is not transferred; the caller retains responsibility
 * for releasing these pointers.
 */
int lance_writer_write_batch(
    LanceWriter* writer,
    const void* arrow_array_ptr,
    const void* arrow_schema_ptr
);

/**
 * Finalize and close the Lance writer.
 *
 * Writes all accumulated batches to the Lance dataset as a single dataset write,
 * creating the full Lance metadata and fragment structure. No further writes are
 * allowed after calling this.
 *
 * @param writer Pointer to LanceWriter from lance_writer_create()
 *
 * @return 0 on success, non-zero error code on failure:
 *         1 = writer_ptr is null
 *         2 = Writer is already closed
 *         5 = Failed to write Lance dataset
 *         3 = Panic in lance_writer_close
 *
 * After calling this function, you must still call lance_writer_destroy()
 * to free resources.
 */
int lance_writer_close(LanceWriter* writer);

/**
 * Destroy and deallocate the Lance writer.
 *
 * Frees all resources associated with the writer.
 * Do not use the writer pointer after calling this function.
 *
 * @param writer Pointer to LanceWriter from lance_writer_create()
 *
 * The writer should be closed via lance_writer_close() before calling this.
 */
void lance_writer_destroy(LanceWriter* writer);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TPCH_LANCE_FFI_H
