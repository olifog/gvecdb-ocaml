#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <string.h>

#if defined(__SSE2__)
#include <immintrin.h>
#endif

/* SIMD dot product of float32 src[0..dim-1] with float64 query[0..dim-1] */
static inline double dot_f32_f64(const float *src, const double *query,
                                 intnat dim) {
    double sum = 0.0;
    intnat i = 0;

#if defined(__AVX__)
    {
        __m256d acc0 = _mm256_setzero_pd();
        __m256d acc1 = _mm256_setzero_pd();

        for (; i + 7 < dim; i += 8) {
            __m128 f32_lo = _mm_loadu_ps(src + i);
            __m128 f32_hi = _mm_loadu_ps(src + i + 4);
            __m256d d0 = _mm256_cvtps_pd(f32_lo);
            __m256d d1 = _mm256_cvtps_pd(f32_hi);
            __m256d q0 = _mm256_loadu_pd(query + i);
            __m256d q1 = _mm256_loadu_pd(query + i + 4);
#if defined(__FMA__)
            acc0 = _mm256_fmadd_pd(d0, q0, acc0);
            acc1 = _mm256_fmadd_pd(d1, q1, acc1);
#else
            acc0 = _mm256_add_pd(acc0, _mm256_mul_pd(d0, q0));
            acc1 = _mm256_add_pd(acc1, _mm256_mul_pd(d1, q1));
#endif
        }

        acc0 = _mm256_add_pd(acc0, acc1);
        __m128d lo = _mm256_castpd256_pd128(acc0);
        __m128d hi = _mm256_extractf128_pd(acc0, 1);
        lo = _mm_add_pd(lo, hi);
        __m128d hi64 = _mm_unpackhi_pd(lo, lo);
        lo = _mm_add_sd(lo, hi64);
        _mm_store_sd(&sum, lo);
    }
#elif defined(__SSE2__)
    {
        __m128d acc0 = _mm_setzero_pd();
        __m128d acc1 = _mm_setzero_pd();

        for (; i + 3 < dim; i += 4) {
            __m128 f32 = _mm_loadu_ps(src + i);
            __m128d d0 = _mm_cvtps_pd(f32);
            __m128d d1 = _mm_cvtps_pd(_mm_movehl_ps(f32, f32));
            __m128d q0 = _mm_loadu_pd(query + i);
            __m128d q1 = _mm_loadu_pd(query + i + 2);
            acc0 = _mm_add_pd(acc0, _mm_mul_pd(d0, q0));
            acc1 = _mm_add_pd(acc1, _mm_mul_pd(d1, q1));
        }

        acc0 = _mm_add_pd(acc0, acc1);
        __m128d hi = _mm_unpackhi_pd(acc0, acc0);
        acc0 = _mm_add_sd(acc0, hi);
        _mm_store_sd(&sum, acc0);
    }
#endif

    /* scalar tail */
    for (; i < dim; i++) {
        sum += (double)src[i] * query[i];
    }

    return sum;
}

double gvecdb_dot_f32_f64(value v_bs, value v_arr, intnat dim) {
    const float *src = (const float *)Caml_ba_data_val(v_bs);
    const double *query = (const double *)Op_val(v_arr);
    return dot_f32_f64(src, query, dim);
}

CAMLprim value gvecdb_dot_f32_f64_bc(value v_bs, value v_arr, value v_dim) {
    return caml_copy_double(gvecdb_dot_f32_f64(v_bs, v_arr, Int_val(v_dim)));
}

/* combined distance-from-mmap, reads vector header + float32 data directly
   from the mmap pointer, computes SIMD dot product, applies distance metric

   vector header layout (16 bytes):
     +0: dim (int32_le)
     +4: flags (uint8), bit 0 = normalized
     +5: reserved (3 bytes)
     +8: norm (float64_le)
    +16: float32 data[dim]

   metric encoding: 0=Euclidean, 1=Cosine, 2=DotProduct
*/
double gvecdb_dist_from_mmap(value v_mmap, value v_query,
                             intnat byte_offset, double query_norm,
                             intnat metric, intnat dim) {
    const char *base = (const char *)Caml_ba_data_val(v_mmap);
    const char *hdr = base + byte_offset;

    double vec_norm;
    memcpy(&vec_norm, hdr + 8, sizeof(double));

    const float *src = (const float *)(hdr + 16);
    const double *query = (const double *)Op_val(v_query);

    double norm_dot = dot_f32_f64(src, query, dim);

    switch (metric) {
        case 1: /* cosine: 1 - dot(normalized_query, normalized_vec) */
            return 1.0 - norm_dot;
        case 0: { /* euclidean: ||a||^2 + ||b||^2 - 2*||a||*||b||*dot */
            double dot = query_norm * vec_norm * norm_dot;
            double qn2 = query_norm * query_norm;
            double vn2 = vec_norm * vec_norm;
            double d = qn2 + vn2 - 2.0 * dot;
            return d > 0.0 ? d : 0.0;
        }
        case 2: /* dotproduct: -(||a||*||b||*dot) */
            return -(query_norm * vec_norm * norm_dot);
        default:
            return 1.0 / 0.0; /* infinity */
    }
}

CAMLprim value gvecdb_dist_from_mmap_bc(value *argv, int argn) {
    (void)argn;
    return caml_copy_double(
        gvecdb_dist_from_mmap(argv[0], argv[1],
                              Int_val(argv[2]), Double_val(argv[3]),
                              Int_val(argv[4]), Int_val(argv[5]))
    );
}

static inline double dot_f32_f32(const float *src, const float *query,
                                 intnat dim) {
    float sum = 0.0f;
    intnat i = 0;

#if defined(__AVX__)
    {
        __m256 acc0 = _mm256_setzero_ps();
        __m256 acc1 = _mm256_setzero_ps();

        for (; i + 15 < dim; i += 16) {
            __m256 s0 = _mm256_loadu_ps(src + i);
            __m256 s1 = _mm256_loadu_ps(src + i + 8);
            __m256 q0 = _mm256_loadu_ps(query + i);
            __m256 q1 = _mm256_loadu_ps(query + i + 8);
#if defined(__FMA__)
            acc0 = _mm256_fmadd_ps(s0, q0, acc0);
            acc1 = _mm256_fmadd_ps(s1, q1, acc1);
#else
            acc0 = _mm256_add_ps(acc0, _mm256_mul_ps(s0, q0));
            acc1 = _mm256_add_ps(acc1, _mm256_mul_ps(s1, q1));
#endif
        }

        for (; i + 7 < dim; i += 8) {
            __m256 s0 = _mm256_loadu_ps(src + i);
            __m256 q0 = _mm256_loadu_ps(query + i);
#if defined(__FMA__)
            acc0 = _mm256_fmadd_ps(s0, q0, acc0);
#else
            acc0 = _mm256_add_ps(acc0, _mm256_mul_ps(s0, q0));
#endif
        }

        acc0 = _mm256_add_ps(acc0, acc1);
        /* horizontal sum of 8 floats */
        __m128 lo = _mm256_castps256_ps128(acc0);
        __m128 hi = _mm256_extractf128_ps(acc0, 1);
        lo = _mm_add_ps(lo, hi);
        lo = _mm_hadd_ps(lo, lo);
        lo = _mm_hadd_ps(lo, lo);
        _mm_store_ss(&sum, lo);
    }
#elif defined(__SSE2__)
    {
        __m128 acc0 = _mm_setzero_ps();
        __m128 acc1 = _mm_setzero_ps();

        for (; i + 7 < dim; i += 8) {
            __m128 s0 = _mm_loadu_ps(src + i);
            __m128 s1 = _mm_loadu_ps(src + i + 4);
            __m128 q0 = _mm_loadu_ps(query + i);
            __m128 q1 = _mm_loadu_ps(query + i + 4);
            acc0 = _mm_add_ps(acc0, _mm_mul_ps(s0, q0));
            acc1 = _mm_add_ps(acc1, _mm_mul_ps(s1, q1));
        }

        acc0 = _mm_add_ps(acc0, acc1);
        /* horizontal sum of 4 floats */
        __m128 shuf = _mm_movehdup_ps(acc0);
        acc0 = _mm_add_ps(acc0, shuf);
        shuf = _mm_movehl_ps(shuf, acc0);
        acc0 = _mm_add_ss(acc0, shuf);
        _mm_store_ss(&sum, acc0);
    }
#endif

    for (; i < dim; i++) {
        sum += src[i] * query[i];
    }

    return (double)sum;
}

double gvecdb_dist_from_mmap_f32(value v_mmap, value v_query_f32,
                                  intnat byte_offset, double query_norm,
                                  intnat metric, intnat dim) {
    const char *base = (const char *)Caml_ba_data_val(v_mmap);
    const char *hdr = base + byte_offset;

    double vec_norm;
    memcpy(&vec_norm, hdr + 8, sizeof(double));

    const float *src = (const float *)(hdr + 16);
    const float *query = (const float *)Caml_ba_data_val(v_query_f32);

    double norm_dot = dot_f32_f32(src, query, dim);

    switch (metric) {
        case 1: /* cosine: 1 - dot(normalized_query, normalized_vec) */
            return 1.0 - norm_dot;
        case 0: { /* euclidean: ||a||^2 + ||b||^2 - 2*||a||*||b||*dot */
            double dot = query_norm * vec_norm * norm_dot;
            double qn2 = query_norm * query_norm;
            double vn2 = vec_norm * vec_norm;
            double d = qn2 + vn2 - 2.0 * dot;
            return d > 0.0 ? d : 0.0;
        }
        case 2: /* dotproduct: -(||a||*||b||*dot) */
            return -(query_norm * vec_norm * norm_dot);
        default:
            return 1.0 / 0.0; /* infinity */
    }
}

CAMLprim value gvecdb_dist_from_mmap_f32_bc(value *argv, int argn) {
    (void)argn;
    return caml_copy_double(
        gvecdb_dist_from_mmap_f32(argv[0], argv[1],
                                   Int_val(argv[2]), Double_val(argv[3]),
                                   Int_val(argv[4]), Int_val(argv[5]))
    );
}
