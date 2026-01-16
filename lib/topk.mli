(** max-heap for top-k selection. O(n log k) for n insertions, O(k) space *)

type 'a t

val create : int -> 'a t
val insert : 'a t -> float -> 'a -> unit
val worst_dist : 'a t -> float
val length : 'a t -> int
val is_full : 'a t -> bool
val to_sorted_list : 'a t -> (float * 'a) list
val iter : (float -> 'a -> unit) -> 'a t -> unit
