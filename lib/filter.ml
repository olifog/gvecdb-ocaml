open Types

type filter_op = Eq | Neq | Lt | Gt | Lte | Gte

type filter_predicate = {
  field_name : string;
  op : filter_op;
  value : Dynamic_reader.field_value;
}

let compare_field_values (a : Dynamic_reader.field_value)
    (b : Dynamic_reader.field_value) : int option =
  let open Dynamic_reader in
  match (a, b) with
  | V_void, V_void -> Some 0
  | V_bool a, V_bool b -> Some (Bool.compare a b)
  | V_int8 a, V_int8 b -> Some (Int.compare a b)
  | V_int16 a, V_int16 b -> Some (Int.compare a b)
  | V_int32 a, V_int32 b -> Some (Int32.compare a b)
  | V_int64 a, V_int64 b -> Some (Int64.compare a b)
  | V_uint8 a, V_uint8 b -> Some (Int.compare a b)
  | V_uint16 a, V_uint16 b -> Some (Int.compare a b)
  | V_uint32 a, V_uint32 b -> Some (Int32.unsigned_compare a b)
  | V_uint64 a, V_uint64 b -> Some (Int64.unsigned_compare a b)
  | V_float32 a, V_float32 b -> Some (Float.compare a b)
  | V_float64 a, V_float64 b -> Some (Float.compare a b)
  | V_text a, V_text b -> Some (String.compare a b)
  | V_data a, V_data b -> Some (String.compare a b)
  | _ -> None

let eval_op op cmp =
  match op with
  | Eq -> cmp = 0
  | Neq -> cmp <> 0
  | Lt -> cmp < 0
  | Gt -> cmp > 0
  | Lte -> cmp <= 0
  | Gte -> cmp >= 0

type prepared_filter = {
  predicates :
    (Schema_registry.field_descriptor * filter_op * Dynamic_reader.field_value)
    list;
}

let prepare_filter (schema : Schema_registry.registered_schema)
    (preds : filter_predicate list) : (prepared_filter, error) result =
  let rec resolve acc = function
    | [] -> Ok { predicates = List.rev acc }
    | pred :: rest -> (
        match Hashtbl.find_opt schema.field_by_name pred.field_name with
        | None ->
            Error
              (Storage_error ("unknown field in filter: " ^ pred.field_name))
        | Some fd -> resolve ((fd, pred.op, pred.value) :: acc) rest)
  in
  resolve [] preds

let eval_prepared (bs : Common.bigstring) ~data_start ~ptr_start
    (pf : prepared_filter) : bool =
  List.for_all
    (fun (fd, op, target) ->
      let actual = Dynamic_reader.read_field bs ~data_start ~ptr_start fd in
      match compare_field_values actual target with
      | None -> false
      | Some cmp -> eval_op op cmp)
    pf.predicates

let matches_blob (bs : Common.bigstring) (pf : prepared_filter) : bool =
  match Dynamic_reader.parse_struct_sections bs with
  | Error _ -> false
  | Ok (data_start, _dw, ptr_start, _pc) ->
      eval_prepared bs ~data_start ~ptr_start pf
