type t = { k : int; heap : Int_heap.t }

let create k =
  if k <= 0 then invalid_arg "Int_topk.create: k must be positive";
  { k; heap = Int_heap.create ~initial_capacity:k Int_heap.Max }

let insert h dist value =
  if Int_heap.length h.heap < h.k then Int_heap.push h.heap dist value
  else if dist < Int_heap.peek_dist h.heap then (
    ignore (Int_heap.pop h.heap);
    Int_heap.push h.heap dist value)

let worst_dist h = Int_heap.peek_dist h.heap
let length h = Int_heap.length h.heap
let is_full h = Int_heap.length h.heap >= h.k
let to_sorted_list h = Int_heap.to_sorted_list h.heap
