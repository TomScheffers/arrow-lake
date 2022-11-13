use arrow2::{
    types::{NativeType},
    scalar::PrimitiveScalar,
    array::{PrimitiveArray, BooleanArray},
    compute::comparison::{eq_scalar, neq_scalar, lt_eq_scalar, lt_scalar, gt_scalar, gt_eq_scalar},
};

pub enum FilterPredicate<T: NativeType> {
    LessEqual(PrimitiveScalar<T>),
    Less(PrimitiveScalar<T>),
    Greater(PrimitiveScalar<T>),
    GreaterEqual(PrimitiveScalar<T>),
    Equal(PrimitiveScalar<T>),
    NotEqual(PrimitiveScalar<T>),
}

pub fn filter_array<T: NativeType>(array: &PrimitiveArray<T>, filter: &FilterPredicate<T>) -> BooleanArray {
    match filter {
        FilterPredicate::LessEqual(value) => lt_eq_scalar(array, value),
        FilterPredicate::Less(value) => lt_scalar(array, value),
        FilterPredicate::Greater(value) => gt_scalar(array, value),
        FilterPredicate::GreaterEqual(value) => gt_eq_scalar(array, value),
        FilterPredicate::Equal(value) => eq_scalar(array, value),
        FilterPredicate::NotEqual(value) => neq_scalar(array, value)
    }
}

#[cfg(test)]
mod tests {
    use arrow2::array::{BooleanArray, Int32Array};
    use arrow2::datatypes::DataType;
    use super::*;

    #[test]
    fn test_filter() {
        let array = Int32Array::from(&[Some(1), None, Some(3)]);
        let scalar = PrimitiveScalar::new(DataType::Int32, Some(2));
        let filter = FilterPredicate::LessEqual(scalar);
        let filter_arr = filter_array(&array, &filter);
        
        assert_eq!(filter_arr, BooleanArray::from(&[Some(true), None, Some(false)]));
    }
}
