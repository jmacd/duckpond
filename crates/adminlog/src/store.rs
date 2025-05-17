use arrow::datatypes::FieldRef;

pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;
}
