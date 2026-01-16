type t = Common.bigstring

let dim (v : t) : int = Bigstringaf.length v / 4

let get (v : t) (i : int) : float =
  Int32.float_of_bits (Bigstringaf.get_int32_le v (i * 4))

let set (v : t) (i : int) (x : float) : unit =
  Bigstringaf.set_int32_le v (i * 4) (Int32.bits_of_float x)

let fold (f : float -> 'a -> 'a) (v : t) (init : 'a) : 'a =
  let n = dim v in
  let acc = ref init in
  for i = 0 to n - 1 do
    acc := f (get v i) !acc
  done;
  !acc

let iter (f : float -> unit) (v : t) : unit =
  let n = dim v in
  for i = 0 to n - 1 do
    f (get v i)
  done

let norm_sq (v : t) : float =
  let n = dim v in
  let sum = ref 0.0 in
  for i = 0 to n - 1 do
    let x = get v i in
    sum := !sum +. (x *. x)
  done;
  !sum

let dot_with_array (v : t) (arr : float array) : float =
  let n = dim v in
  if n <> Array.length arr then
    invalid_arg
      (Printf.sprintf
         "Float32_vec.dot_with_array: dimension mismatch (%d vs %d)" n
         (Array.length arr))
  else begin
    let sum = ref 0.0 in
    for i = 0 to n - 1 do
      sum := !sum +. (get v i *. arr.(i))
    done;
    !sum
  end

let dot (v1 : t) (v2 : t) : float =
  let n = dim v1 in
  if n <> dim v2 then
    invalid_arg
      (Printf.sprintf "Float32_vec.dot: dimension mismatch (%d vs %d)" n
         (dim v2))
  else begin
    let sum = ref 0.0 in
    for i = 0 to n - 1 do
      sum := !sum +. (get v1 i *. get v2 i)
    done;
    !sum
  end

let to_array (v : t) : float array = Array.init (dim v) (fun i -> get v i)

let of_array (arr : float array) : t =
  let n = Array.length arr in
  let v = Bigstringaf.create (n * 4) in
  for i = 0 to n - 1 do
    set v i arr.(i)
  done;
  v

let same_dim (v1 : t) (v2 : t) : bool = dim v1 = dim v2
let has_dim (v : t) (d : int) : bool = dim v = d

let normalize (v : t) : t * float =
  let ns = norm_sq v in
  let norm = sqrt ns in
  if norm = 0.0 then (v, 0.0)
  else begin
    let n = dim v in
    let out = Bigstringaf.create (n * 4) in
    let inv_norm = 1.0 /. norm in
    for i = 0 to n - 1 do
      set out i (get v i *. inv_norm)
    done;
    (out, norm)
  end
