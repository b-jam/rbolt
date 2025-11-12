use std::cmp::Ordering;
use zerocopy::FromBytes;
use crate::page::{LeafElement, BranchElement};

pub fn binary_search<F>(start: usize, end: usize, mut compare: F) -> Result<(usize, bool), ()>
where
    F: FnMut(usize) -> Result<Ordering, ()>,
{
    let mut left = start;
    let mut right = end;
    let mut insert_pos = end;

    while left < right {
        let mid = left + (right - left) / 2;

        match compare(mid)? {
            Ordering::Equal => return Ok((mid, true)),
            Ordering::Less => {
                left = mid + 1;
                insert_pos = left;
            }
            Ordering::Greater => {
                right = mid;
                insert_pos = mid;
            }
        }
    }

    Ok((insert_pos, false))
}

pub fn search_leaf_elements(
    page_body: &[u8],
    element_count: usize,
    search_key: &[u8],
) -> Result<(usize, bool), ()> {
    let element_size = std::mem::size_of::<LeafElement>();

    binary_search(0, element_count, |mid| {
        let elem_bytes = &page_body[(mid*element_size)..(mid+1)*element_size];
        let elem = LeafElement::ref_from_bytes(elem_bytes).map_err(|_| ())?;

        let key_start = elem.kptr as usize;
        let key_end = key_start + elem.ksize as usize;
        let stored_key = &page_body[key_start..key_end];

        Ok(stored_key.cmp(search_key))
    })
}

pub fn search_branch_elements(
    page_body: &[u8],
    element_count: usize,
    search_key: &[u8],
) -> Result<(usize, bool), ()> {
    let element_size = std::mem::size_of::<BranchElement>();

    binary_search(1, element_count + 1, |mid| {
        let elem_bytes = &page_body[(mid*element_size)..(mid+1)*element_size];
        let elem = BranchElement::ref_from_bytes(elem_bytes).map_err(|_| ())?;

        if elem.ksize == 0 {
            return Ok(Ordering::Greater);
        }

        let key_start = elem.kptr as usize;
        let key_end = key_start + elem.ksize as usize;
        let key_data = &page_body[key_start..key_end];

        Ok(key_data.cmp(search_key))
    })
}
