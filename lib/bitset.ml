type t = Bytes.t

let create n =
  let len = (n + 7) asr 3 in
  Bytes.make len '\x00'

let test_and_set t i =
  let byte_idx = i asr 3 in
  if byte_idx < 0 || byte_idx >= Bytes.length t then true
  else
    let bit = 1 lsl (i land 7) in
    let old = Char.code (Bytes.unsafe_get t byte_idx) in
    if old land bit <> 0 then true
    else begin
      Bytes.unsafe_set t byte_idx (Char.unsafe_chr (old lor bit));
      false
    end
