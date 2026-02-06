type params = {
  m : int;
  m_max : int;
  ef_construction : int;
  max_layers : int;
  ml : float;
}

let default_params =
  let m = 16 in
  {
    m;
    m_max = 8;
    ef_construction = 200;
    max_layers = 5;
    ml = 1.0 /. log (float_of_int m);
  }
