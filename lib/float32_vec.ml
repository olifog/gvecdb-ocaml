module F32u = Stdlib_stable.Float32_u
module F32x8 = Ocaml_simd_avx.Float32x8

external box_float : float# -> float = "%box_float"
external unbox_float : float -> float# = "%unbox_float"

let f32_to_f64 (x : float32#) : float = box_float (F32u.to_float x)
let f64_to_f32 (x : float) : float32# = F32u.of_float (unbox_float x)

type t = Common.bigstring

let dim (v : t) : int = Bigstringaf.length v / 4
let get32 (v : t) (i : int) : float32# = F32u.Bigstring.unsafe_get v ~pos:(i * 4)

let set32 (v : t) (i : int) (x : float32#) : unit =
  F32u.Bigstring.unsafe_set v ~pos:(i * 4) x

let set (v : t) (i : int) (x : float) : unit =
  Bigstringaf.set_int32_le v (i * 4) (Int32.bits_of_float x)

let dot_raw (a : t) ~(a_off : int) (b : t) ~(b_off : int) ~(dim : int) : float =
  let n8 = dim land lnot 7 in
  let chunks = n8 / 8 in
  let rec simd_loop i (acc : float32x8#) =
    if i >= chunks then acc
    else
      let byte_a = a_off + (i * 32) in
      let byte_b = b_off + (i * 32) in
      let va = F32x8.Bigstring.unsafe_unaligned_get a ~byte:byte_a in
      let vb = F32x8.Bigstring.unsafe_unaligned_get b ~byte:byte_b in
      simd_loop (i + 1) (F32x8.mul_add va vb acc)
  in
  let acc = simd_loop 0 F32x8.zero in
  let hsum = F32x8.dot acc (F32x8.set1 #1.0s) in
  let rec scalar_loop i (sum : float32#) =
    if i >= dim then sum
    else
      let va = F32u.Bigstring.unsafe_get a ~pos:(a_off + (i * 4)) in
      let vb = F32u.Bigstring.unsafe_get b ~pos:(b_off + (i * 4)) in
      scalar_loop (i + 1) (F32u.add sum (F32u.mul va vb))
  in
  f32_to_f64 (scalar_loop n8 hsum)

let dot (v1 : t) (v2 : t) : float =
  let n = dim v1 in
  dot_raw v1 ~a_off:0 v2 ~b_off:0 ~dim:n

let norm_sq (v : t) : float = dot v v

let normalize (v : t) : t * float =
  let ns = norm_sq v in
  let norm = sqrt ns in
  if norm = 0.0 then (v, 0.0)
  else
    let n = dim v in
    let out = Bigstringaf.create (n * 4) in
    let inv_norm = f64_to_f32 (1.0 /. norm) in
    for i = 0 to n - 1 do
      set32 out i (F32u.mul (get32 v i) inv_norm)
    done;
    (out, norm)

let of_array (arr : float array) : t =
  let n = Array.length arr in
  let v = Bigstringaf.create (n * 4) in
  for i = 0 to n - 1 do
    set v i arr.(i)
  done;
  v

let dist_from_header (mmap : t) ~(vec_off : int) (query : t)
    ~(query_norm : float) ~(metric : int) ~(dim : int) : float =
  let vec_norm =
    Int64.float_of_bits (Bigstringaf.get_int64_le mmap (vec_off + 8))
  in
  let data_off = vec_off + 16 in
  let norm_dot = dot_raw mmap ~a_off:data_off query ~b_off:0 ~dim in
  match metric with
  | 1 -> 1.0 -. norm_dot
  | 0 ->
      let dot = query_norm *. vec_norm *. norm_dot in
      let qn2 = query_norm *. query_norm in
      let vn2 = vec_norm *. vec_norm in
      Float.max 0.0 (qn2 +. vn2 -. (2.0 *. dot))
  | 2 -> -.(query_norm *. vec_norm *. norm_dot)
  | _ -> infinity
