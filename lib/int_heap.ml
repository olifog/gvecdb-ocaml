type order = Min | Max

type t = {
  order : order;
  mutable dists : float array;
  mutable vals : int array;
  mutable size : int;
}

let create ?(initial_capacity = 16) order =
  {
    order;
    dists = Array.make initial_capacity 0.0;
    vals = Array.make initial_capacity 0;
    size = 0;
  }

let length h = h.size
let is_empty h = h.size = 0

let swap h i j =
  let td = h.dists.(i) in
  let tv = h.vals.(i) in
  h.dists.(i) <- h.dists.(j);
  h.vals.(i) <- h.vals.(j);
  h.dists.(j) <- td;
  h.vals.(j) <- tv

let rec sift_up_min h i =
  if i > 0 then
    let p = (i - 1) asr 1 in
    if h.dists.(i) < h.dists.(p) then (
      swap h i p;
      sift_up_min h p)

let rec sift_up_max h i =
  if i > 0 then
    let p = (i - 1) asr 1 in
    if h.dists.(i) > h.dists.(p) then (
      swap h i p;
      sift_up_max h p)

let rec sift_down_min h i =
  let l = (i lsl 1) lor 1 in
  if l < h.size then
    let r = l + 1 in
    let smallest = if r < h.size && h.dists.(r) < h.dists.(l) then r else l in
    if h.dists.(smallest) < h.dists.(i) then (
      swap h i smallest;
      sift_down_min h smallest)

let rec sift_down_max h i =
  let l = (i lsl 1) lor 1 in
  if l < h.size then
    let r = l + 1 in
    let largest = if r < h.size && h.dists.(r) > h.dists.(l) then r else l in
    if h.dists.(largest) > h.dists.(i) then (
      swap h i largest;
      sift_down_max h largest)

let ensure_capacity h =
  if h.size >= Array.length h.dists then (
    let new_cap = Array.length h.dists * 2 in
    let nd = Array.make new_cap 0.0 in
    let nv = Array.make new_cap 0 in
    Array.blit h.dists 0 nd 0 h.size;
    Array.blit h.vals 0 nv 0 h.size;
    h.dists <- nd;
    h.vals <- nv)

let push h dist value =
  ensure_capacity h;
  let i = h.size in
  h.dists.(i) <- dist;
  h.vals.(i) <- value;
  h.size <- i + 1;
  match h.order with Min -> sift_up_min h i | Max -> sift_up_max h i

let peek_dist h =
  if h.size = 0 then match h.order with Min -> infinity | Max -> neg_infinity
  else h.dists.(0)

let pop h =
  if h.size = 0 then None
  else
    let d = h.dists.(0) in
    let v = h.vals.(0) in
    h.size <- h.size - 1;
    if h.size > 0 then (
      h.dists.(0) <- h.dists.(h.size);
      h.vals.(0) <- h.vals.(h.size);
      match h.order with Min -> sift_down_min h 0 | Max -> sift_down_max h 0);
    Some (d, v)

let to_sorted_list h =
  let n = h.size in
  let pairs = Array.init n (fun i -> (h.dists.(i), h.vals.(i))) in
  Array.sort (fun (d1, _) (d2, _) -> Float.compare d1 d2) pairs;
  Array.to_list pairs
