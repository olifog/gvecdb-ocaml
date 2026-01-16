(** operations on float32 vectors stored as bigstrings (little-endian) *)

type t = Common.bigstring

val dim : t -> int
val get : t -> int -> float
val set : t -> int -> float -> unit

val fold : (float -> 'a -> 'a) -> t -> 'a -> 'a
val iter : (float -> unit) -> t -> unit

val norm_sq : t -> float
val dot : t -> t -> float
val dot_with_array : t -> float array -> float

val to_array : t -> float array
val of_array : float array -> t

val same_dim : t -> t -> bool
val has_dim : t -> int -> bool

val normalize : t -> t * float
(** returns [(unit_vec, original_norm)]. zero vectors return [(v, 0.0)] *)
