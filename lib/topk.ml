type 'a t = { k : int; heap : 'a Heap.t }

let create (k : int) : 'a t =
  if k <= 0 then invalid_arg "TopK.create: k must be positive";
  { k; heap = Heap.create ~initial_capacity:k Heap.Max }

let insert (h : 'a t) (dist : float) (value : 'a) : unit =
  if Heap.length h.heap < h.k then
    Heap.push h.heap dist value
  else if dist < Heap.peek_dist h.heap then begin
    ignore (Heap.pop h.heap);
    Heap.push h.heap dist value
  end

let worst_dist (h : 'a t) : float = Heap.peek_dist h.heap
let length (h : 'a t) : int = Heap.length h.heap
let is_full (h : 'a t) : bool = Heap.length h.heap >= h.k
let to_sorted_list (h : 'a t) : (float * 'a) list = Heap.to_sorted_list h.heap
let iter (f : float -> 'a -> unit) (h : 'a t) : unit = Heap.iter f h.heap
