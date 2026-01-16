type 'a t = { k : int; mutable size : int; data : (float * 'a) array }

let create (k : int) : 'a t =
  if k <= 0 then invalid_arg "TopK.create: k must be positive";
  { k; size = 0; data = Array.make k (infinity, Obj.magic ()) }

(* Sift element at index i up to restore heap property *)
let rec sift_up arr i =
  if i > 0 then begin
    let p = (i - 1) / 2 in
    if fst arr.(i) > fst arr.(p) then begin
      let tmp = arr.(i) in
      arr.(i) <- arr.(p);
      arr.(p) <- tmp;
      sift_up arr p
    end
  end

(* Sift element at index i down to restore heap property *)
let rec sift_down arr size i =
  let l = (2 * i) + 1 and r = (2 * i) + 2 in
  let largest = if l < size && fst arr.(l) > fst arr.(i) then l else i in
  let largest =
    if r < size && fst arr.(r) > fst arr.(largest) then r else largest
  in
  if largest <> i then begin
    let tmp = arr.(i) in
    arr.(i) <- arr.(largest);
    arr.(largest) <- tmp;
    sift_down arr size largest
  end

let insert (h : 'a t) (dist : float) (value : 'a) : unit =
  if h.size < h.k then begin
    (* Heap not full - always insert *)
    h.data.(h.size) <- (dist, value);
    sift_up h.data h.size;
    h.size <- h.size + 1
  end
  else if dist < fst h.data.(0) then begin
    (* Better than worst - replace root and sift down *)
    h.data.(0) <- (dist, value);
    sift_down h.data h.size 0
  end

let worst_dist (h : 'a t) : float =
  if h.size = 0 then infinity else fst h.data.(0)

let length (h : 'a t) : int = h.size
let is_full (h : 'a t) : bool = h.size = h.k

let to_sorted_list (h : 'a t) : (float * 'a) list =
  let items = Array.sub h.data 0 h.size in
  Array.sort (fun (d1, _) (d2, _) -> Float.compare d1 d2) items;
  Array.to_list items

let iter (f : float -> 'a -> unit) (h : 'a t) : unit =
  for i = 0 to h.size - 1 do
    let dist, value = h.data.(i) in
    f dist value
  done
