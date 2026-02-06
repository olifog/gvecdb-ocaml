type order = Min | Max

type 'a t = {
  order : order;
  mutable data : (float * 'a) array;
  mutable size : int;
}

let create ?(initial_capacity = 16) order =
  { order; data = Array.make initial_capacity (0.0, Obj.magic ()); size = 0 }

let length h = h.size
let is_empty h = h.size = 0

let should_swap h parent child =
  match h.order with
  | Min -> fst child < fst parent
  | Max -> fst child > fst parent

let rec sift_up h i =
  if i > 0 then begin
    let p = (i - 1) / 2 in
    if should_swap h h.data.(p) h.data.(i) then begin
      let tmp = h.data.(i) in
      h.data.(i) <- h.data.(p);
      h.data.(p) <- tmp;
      sift_up h p
    end
  end

let rec sift_down h i =
  let l = (2 * i) + 1 and r = (2 * i) + 2 in
  let extreme = ref i in
  if l < h.size && should_swap h h.data.(!extreme) h.data.(l) then extreme := l;
  if r < h.size && should_swap h h.data.(!extreme) h.data.(r) then extreme := r;
  if !extreme <> i then begin
    let tmp = h.data.(i) in
    h.data.(i) <- h.data.(!extreme);
    h.data.(!extreme) <- tmp;
    sift_down h !extreme
  end

let ensure_capacity h =
  if h.size >= Array.length h.data then begin
    let new_cap = Array.length h.data * 2 in
    let new_data = Array.make new_cap (0.0, Obj.magic ()) in
    Array.blit h.data 0 new_data 0 h.size;
    h.data <- new_data
  end

let push h dist value =
  ensure_capacity h;
  h.data.(h.size) <- (dist, value);
  sift_up h h.size;
  h.size <- h.size + 1

let peek h = if h.size = 0 then None else Some h.data.(0)

let peek_dist h =
  if h.size = 0 then match h.order with Min -> infinity | Max -> neg_infinity
  else fst h.data.(0)

let pop h =
  if h.size = 0 then None
  else begin
    let result = h.data.(0) in
    h.size <- h.size - 1;
    if h.size > 0 then begin
      h.data.(0) <- h.data.(h.size);
      sift_down h 0
    end;
    Some result
  end

let to_sorted_list h =
  let items = Array.sub h.data 0 h.size in
  Array.sort (fun (d1, _) (d2, _) -> Float.compare d1 d2) items;
  Array.to_list items

let iter f h =
  for i = 0 to h.size - 1 do
    let dist, value = h.data.(i) in
    f dist value
  done
