use crate::db::{DbError, PAGE_SIZE};
use crate::page::{BRANCH_ELEMENT_SIZE, BranchElement, LEAF_ELEMENT_SIZE, LeafElement, PAGE_BODY_SIZE, PAGE_HEADER_SIZE, Page, PageType};
use crate::search;
use std::collections::HashMap;
use std::sync::{RwLockReadGuard, MutexGuard};
use std::fmt;
use memmap2::MmapMut;
use zerocopy::{FromBytes, IntoBytes};

#[derive(Debug)]
pub enum BTreeError {
    InvalidPageType { page_id: u64, page_type: PageType },
    CorruptPageType { page_id: u64, raw_type: u8 },
    EmptyBranchPage { page_id: u64 },
    KeyTooLarge { key_size: usize, max_size: usize },
    ValueTooLarge { value_size: usize, max_size: usize },
    PageFull { page_id: u64 },
    Db(DbError),
}

impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BTreeError::InvalidPageType { page_id, page_type } => {
                write!(f, "Invalid page type {:?} for page {}", page_type, page_id)
            }
            BTreeError::CorruptPageType { page_id, raw_type } => {
                write!(f, "Corrupt page type byte {} for page {}", raw_type, page_id)
            }
            BTreeError::EmptyBranchPage { page_id } => {
                write!(f, "Branch page {} has no elements", page_id)
            }
            BTreeError::KeyTooLarge { key_size, max_size } => {
                write!(f, "Key size {} exceeds maximum {}", key_size, max_size)
            }
            BTreeError::ValueTooLarge { value_size, max_size } => {
                write!(f, "Value size {} exceeds maximum {}", value_size, max_size)
            }
            BTreeError::PageFull { page_id } => {
                write!(f, "Page {} is full", page_id)
            }
            BTreeError::Db(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for BTreeError {}

impl From<DbError> for BTreeError {
    fn from(err: DbError) -> Self {
        BTreeError::Db(err)
    }
}

type Result<T> = std::result::Result<T, BTreeError>;

pub struct WriteTxn<'a> {
    // So the write guard is when we're actually writing (_write_guard)
    // Most of the time we only need the read lock (mmap_guard), so don't want to block others.
    _write_guard: MutexGuard<'a, ()>,
    mmap_guard: RwLockReadGuard<'a, MmapMut>,
    root_page_id: u64,
    dirty_pages: HashMap<u64, Vec<u8>>,
    free_list: Vec<u64>,
    highest_page_id: u64,
}

impl<'a> WriteTxn<'a> {
    pub fn new(
        write_guard: MutexGuard<'a, ()>,
        mmap_guard: RwLockReadGuard<'a, MmapMut>,
        root_page_id: u64,
        free_list: Vec<u64>,
        highest_page_id: u64,
    ) -> Self {
        WriteTxn {
            _write_guard: write_guard,
            mmap_guard,
            root_page_id,
            dirty_pages: HashMap::new(),
            free_list,
            highest_page_id,
        }
    }
}

impl WriteTxn<'_> {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match self.insert_recursive(self.root_page_id, key, value)? {
            Some((separator_key, new_page_id)) => self.split_root(separator_key, new_page_id),
            None => Ok(()),
        }
    }

    pub fn prepare_commit(mut self) -> (HashMap<u64, Vec<u8>>, u64, u64) {
        let dirty_pages = std::mem::take(&mut self.dirty_pages);
        let highest_page_id = self.highest_page_id;
        let root_page_id = self.root_page_id;
        (dirty_pages, highest_page_id, root_page_id)
    }

    fn insert_recursive(&mut self, page_id: u64, key: &[u8], value: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        let page_type = self.get_page_type(page_id)?;
        match page_type {
            PageType::Leaf => self.insert_into_leaf(page_id, key, value),
            PageType::Branch => {
                let child_page_id = self.find_child_page(page_id, key)?;
                match self.insert_recursive(child_page_id, key, value)? {
                    Some((sep_key, new_child_id)) => self.insert_into_branch(page_id, sep_key, new_child_id),
                    None => Ok(None),
                }
            }
            _ => Err(BTreeError::InvalidPageType {
                page_id,
                page_type,
            }),
        }
    }

    fn find_child_page(&mut self, page_id: u64, for_key: &[u8]) -> Result<u64> {
        let page_bytes = self.read_page(page_id)?;

        let (page_header, page_body) = Page::ref_from_prefix(&page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_bytes[8] })?;
        let element_count = page_header.count as usize;

        let (result_index, found) = search::search_branch_elements(page_body, element_count, for_key)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_bytes[8] })?;

        let child_index = if found {
            result_index
        } else {
            result_index.saturating_sub(1)
        };

        let elem_bytes = &page_body[(child_index*BRANCH_ELEMENT_SIZE)..(child_index+1)*BRANCH_ELEMENT_SIZE];
        let elem = BranchElement::ref_from_bytes(elem_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_bytes[8] })?;

        Ok(elem.page_id)
    }

    fn read_page(&self, page_id: u64) -> Result<&[u8]> {
        if let Some(page_bytes) = self.dirty_pages.get(&page_id) {
            return Ok(page_bytes);
        }
        let offset = (page_id as usize) * PAGE_SIZE;
        if offset + PAGE_SIZE > self.mmap_guard.len() {
            return Err(BTreeError::Db(DbError::PageOutOfBounds {
                page_id,
                file_size: self.mmap_guard.len(),
            }));
        }
        Ok(&self.mmap_guard[offset..offset + PAGE_SIZE])
    }

    fn get_page_for_write(&mut self, page_id: u64) -> Result<&mut [u8]> {
        if !self.dirty_pages.contains_key(&page_id) {
            let page_bytes = self.read_page(page_id)?;
            self.dirty_pages.insert(page_id, page_bytes.to_vec());
        }
        Ok(self.dirty_pages.get_mut(&page_id).unwrap())
    }

    fn get_page_mut(&mut self, page_id: u64) -> Result<(&mut Page, &mut [u8])> {
        let page_bytes = self.get_page_for_write(page_id)?;
        let raw_type = page_bytes[8];
        Page::mut_from_prefix(&mut *page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type })
    }

    fn get_page_immut(&mut self, page_id: u64) -> Result<(&Page, &[u8])> {
        let page_bytes = self.read_page(page_id)?;
        let raw_type = page_bytes[8];
        Page::ref_from_prefix(page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type })
    }

    fn insert_into_leaf(&mut self, page_id: u64, key: &[u8], value: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        if key.len() > u16::MAX as usize {
            return Err(BTreeError::KeyTooLarge { key_size: key.len(), max_size: u16::MAX as usize });
        }
        if value.len() > u16::MAX as usize {
            return Err(BTreeError::ValueTooLarge { value_size: value.len(), max_size: u16::MAX as usize });
        }

        let (page_header, page_body) = self.get_page_mut(page_id)?;
        let current_count = page_header.count as usize;

        // element ptrs are added forwards but the data block is at the end of the page backwards
        let min_kptr = if current_count == 0 {
            PAGE_BODY_SIZE as usize
        } else {
            let mut min_kptr = PAGE_BODY_SIZE;
            for i in 0..current_count {
                let elem = LeafElement::ref_from_bytes(&page_body[i*LEAF_ELEMENT_SIZE..(i+1)*LEAF_ELEMENT_SIZE])
                    .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;
                min_kptr = min_kptr.min(elem.kptr as usize);
            }
            min_kptr
        };

        let new_elements_end = (current_count + 1) * LEAF_ELEMENT_SIZE;
        let key_offset = min_kptr - (key.len() + value.len());
        let value_offset = key_offset + key.len();

        if new_elements_end > key_offset {
            return self.split_leaf(page_id, key, value);
        }

        let (insert_pos, found) = search::search_leaf_elements(page_body, current_count, key)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;

        page_body[key_offset..value_offset].copy_from_slice(key);
        page_body[value_offset..value_offset + value.len()].copy_from_slice(value);

        let leaf_element = LeafElement {
            ksize: key.len() as u16,
            vsize: value.len() as u16,
            kptr: key_offset as u16,
            vptr: value_offset as u16,
        };

        let elem_offset = insert_pos * LEAF_ELEMENT_SIZE;
        if found {
            page_body[elem_offset..elem_offset + LEAF_ELEMENT_SIZE]
                .copy_from_slice(leaf_element.as_bytes());
            println!("   [OK] Updated key (len={}) value (len={}) in page {} at position {}",
                     key.len(), value.len(), page_id, insert_pos);
            return Ok(None);
        }

        if insert_pos < current_count {
            //shift elements to make room
            page_body.copy_within(
                insert_pos * LEAF_ELEMENT_SIZE..current_count * LEAF_ELEMENT_SIZE,
                (insert_pos + 1) * LEAF_ELEMENT_SIZE
            );
        }

        page_body[elem_offset..elem_offset + LEAF_ELEMENT_SIZE]
            .copy_from_slice(leaf_element.as_bytes());

        page_header.count = (current_count + 1) as u16;

        println!("   [OK] Inserted key (len={}) value (len={}) into page {} at position {}, count now {}",
                 key.len(), value.len(), page_id, insert_pos, current_count + 1);
        Ok(None)
    }

    fn split_leaf(&mut self, page_id: u64, new_key: &[u8], new_value: &[u8]) -> Result<Option<(Vec<u8>, u64)>> {
        println!("   [SPLIT] Splitting leaf page {}", page_id);

        let (page_header, page_body) = self.get_page_immut(page_id)?;
        let count = page_header.count as usize;

        let mut kvs = Vec::with_capacity(count + 1);
        let mut inserted = false;

        for i in 0..count {
            let elem = LeafElement::ref_from_bytes(&page_body[i*LEAF_ELEMENT_SIZE..(i+1)*LEAF_ELEMENT_SIZE])
                .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;

            let key = &page_body[elem.kptr as usize..(elem.kptr + elem.ksize) as usize];
            let value = &page_body[elem.vptr as usize..(elem.vptr + elem.vsize) as usize];

            if !inserted && new_key < key {
                kvs.push((new_key.to_vec(), new_value.to_vec()));
                inserted = true;
            }
            kvs.push((key.to_vec(), value.to_vec()));
        }

        if !inserted {
            kvs.push((new_key.to_vec(), new_value.to_vec()));
        }

        let split_idx = (kvs.len() + 1) / 2;
        let new_page_id = self.allocate_page()?;
        self.write_leaf_page(page_id, &kvs[..split_idx])?;
        self.write_leaf_page(new_page_id, &kvs[split_idx..])?;
        let separator = kvs[split_idx].0.clone();

        println!("   [SPLIT] Split into pages {} and {}, separator key len={}",
                 page_id, new_page_id, separator.len());
        Ok(Some((separator, new_page_id)))
    }

    // (key, child_page_id). The first entry is child only, empty key
    fn write_branch_page(&mut self, page_id: u64, entries: &[(Vec<u8>, u64)]) -> Result<()> {
        let mut page_bytes = vec![0u8; PAGE_SIZE];

        let page = Page {
            id: page_id,
            page_type: PageType::Branch as u8,
            _padding: 0,
            count: (entries.len() - 1) as u16,
            overflow: 0,
        };
        page_bytes[..PAGE_HEADER_SIZE].copy_from_slice(page.as_bytes());

        let mut data_offset = PAGE_BODY_SIZE;
        for (i, (key, child_id)) in entries.iter().enumerate() {
            let kptr = match key.is_empty() {
                true => 0,
                false => {
                    data_offset -= key.len();
                    page_bytes[data_offset..data_offset + key.len()].copy_from_slice(key);
                    data_offset
                }
            };
            let elem = BranchElement {
                page_id: *child_id,
                ksize: key.len() as u16,
                kptr: kptr as u16,
                _padding: [0; 4],
            };
            page_bytes[(PAGE_HEADER_SIZE + i*BRANCH_ELEMENT_SIZE)..(PAGE_HEADER_SIZE + (i+1)*BRANCH_ELEMENT_SIZE)]
                .copy_from_slice(elem.as_bytes());
        }
        self.dirty_pages.insert(page_id, page_bytes);
        Ok(())
    }

    fn write_leaf_page(&mut self, page_id: u64, kvs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let mut page_bytes = vec![0u8; PAGE_SIZE];
        let page = Page {
            id: page_id,
            page_type: PageType::Leaf as u8,
            _padding: 0,
            count: kvs.len() as u16,
            overflow: 0,
        };
        page_bytes[..PAGE_HEADER_SIZE].copy_from_slice(page.as_bytes());
        let mut data_offset = PAGE_SIZE;

        for (i, (key, value)) in kvs.iter().enumerate() {
            data_offset -= value.len();
            page_bytes[data_offset..data_offset + value.len()].copy_from_slice(value);
            let vptr_body = data_offset - PAGE_HEADER_SIZE;

            data_offset -= key.len();
            page_bytes[data_offset..data_offset + key.len()].copy_from_slice(key);
            let kptr_body = data_offset - PAGE_HEADER_SIZE;

            let elem = LeafElement {
                ksize: key.len() as u16,
                vsize: value.len() as u16,
                kptr: kptr_body as u16,
                vptr: vptr_body as u16,
            };
            let offset = PAGE_HEADER_SIZE + i * LEAF_ELEMENT_SIZE;
            page_bytes[offset..offset + LEAF_ELEMENT_SIZE].copy_from_slice(elem.as_bytes());
        }

        self.dirty_pages.insert(page_id, page_bytes);
        Ok(())
    }

    fn split_root(&mut self, separator_key: Vec<u8>, new_page_id: u64) -> Result<()> {
        let old_root_id = self.root_page_id;
        let new_root_id = self.allocate_page()?;

        println!("   [SPLIT] Splitting root {} into new root {} with children {} and {}",
                 old_root_id, new_root_id, old_root_id, new_page_id);
        let mut page_bytes = vec![0u8; PAGE_SIZE];

        let key_offset = PAGE_BODY_SIZE - separator_key.len();
        page_bytes[PAGE_HEADER_SIZE + key_offset..].copy_from_slice(&separator_key);

        let page = Page {
            id: new_root_id,
            page_type: PageType::Branch as u8,
            _padding: 0,
            count: 1,  // One separator key
            overflow: 0,
        };
        page_bytes[..PAGE_HEADER_SIZE].copy_from_slice(page.as_bytes());

        let elem = BranchElement {
            page_id: old_root_id,
            ksize: 0,
            kptr: 0,
            _padding: [0; 4],
        };
        page_bytes[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + BRANCH_ELEMENT_SIZE].copy_from_slice(elem.as_bytes());
        let elem2 = BranchElement {
            page_id: new_page_id,
            ksize: separator_key.len() as u16,
            kptr: key_offset as u16,
            _padding: [0; 4],
        };
        page_bytes[PAGE_HEADER_SIZE + BRANCH_ELEMENT_SIZE..PAGE_HEADER_SIZE + 2 * BRANCH_ELEMENT_SIZE]
            .copy_from_slice(elem2.as_bytes());

        self.dirty_pages.insert(new_root_id, page_bytes);
        self.root_page_id = new_root_id;
        Ok(())
    }

    fn insert_into_branch(&mut self, page_id: u64, key: Vec<u8>, child_page_id: u64) -> Result<Option<(Vec<u8>, u64)>> {
        let (page_header, page_body) = self.get_page_mut(page_id)?;
        let current_count = page_header.count as usize;
        let total_elements = current_count + 1;

        let min_kptr = if current_count == 0 {
            PAGE_BODY_SIZE
        } else {
            let mut min_kptr = PAGE_BODY_SIZE;
            for i in 0..total_elements {
                let elem_bytes = &page_body[i*BRANCH_ELEMENT_SIZE..(i+1)*BRANCH_ELEMENT_SIZE];
                let elem = BranchElement::ref_from_bytes(elem_bytes)
                    .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;
                if elem.ksize > 0 {
                    min_kptr = min_kptr.min(elem.kptr as usize);
                }
            }
            min_kptr
        };

        let new_elements_end = (total_elements + 1) * BRANCH_ELEMENT_SIZE;
        let key_offset = min_kptr - key.len();

        if new_elements_end > key_offset {
            return self.split_branch(page_id, key, child_page_id);
        }

        let (insert_pos, _) = search::search_branch_elements(page_body, total_elements, &key)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;

        page_body[key_offset..key_offset + key.len()].copy_from_slice(&key);

        if insert_pos < total_elements {
            page_body.copy_within(insert_pos * BRANCH_ELEMENT_SIZE..total_elements * BRANCH_ELEMENT_SIZE,
                (insert_pos + 1) * BRANCH_ELEMENT_SIZE);
        }

        let new_element = BranchElement {
            page_id: child_page_id,
            ksize: key.len() as u16,
            kptr: key_offset as u16,
            _padding: [0; 4],
        };

        page_body[insert_pos*BRANCH_ELEMENT_SIZE..(insert_pos+1)*BRANCH_ELEMENT_SIZE].copy_from_slice(new_element.as_bytes());
        page_header.count = (current_count + 1) as u16;

        println!("   [OK] Inserted separator key (len={}) into branch page {}, count now {}",
                 key.len(), page_id, page_header.count);

        Ok(None)
    }

    fn split_branch(&mut self, page_id: u64, new_key: Vec<u8>, new_child_id: u64) -> Result<Option<(Vec<u8>, u64)>> {
        println!("   [SPLIT] Splitting branch page {}", page_id);

        let (page_header, page_body) = self.get_page_immut(page_id)?;
        let count = page_header.count as usize;

        // branch has count+1 children (first has no key)
        let mut entries = Vec::with_capacity(count + 2);
        let mut inserted = false;

        let elem = BranchElement::ref_from_bytes(&page_body[0..BRANCH_ELEMENT_SIZE])
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;
        entries.push((Vec::new(), elem.page_id));

        for i in 1..=count {
            let elem = BranchElement::ref_from_bytes(&page_body[i*BRANCH_ELEMENT_SIZE..(i+1)*BRANCH_ELEMENT_SIZE])
                .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: page_header.page_type })?;

            let key = page_body[elem.kptr as usize..(elem.kptr + elem.ksize) as usize].to_vec();

            if !inserted && new_key.as_slice() < key.as_slice() {
                entries.push((new_key.clone(), new_child_id));
                inserted = true;
            }
            entries.push((key, elem.page_id));
        }

        if !inserted {
            entries.push((new_key.clone(), new_child_id));
        }

        let split_idx = entries.len() / 2;
        let separator = entries[split_idx].0.clone();
        let new_page_id = self.allocate_page()?;
        self.write_branch_page(page_id, &entries[0..split_idx])?;
        let mut right_entries = vec![(Vec::new(), entries[split_idx].1)];
        right_entries.extend_from_slice(&entries[split_idx + 1..]);
        self.write_branch_page(new_page_id, &right_entries)?;

        println!("   [SPLIT] Split branch into pages {} and {}, separator key len={}",
                 page_id, new_page_id, separator.len());
        Ok(Some((separator, new_page_id)))
    }

    fn allocate_page(&mut self) -> Result<u64> {
        if let Some(page_id) = self.free_list.pop() {
            return Ok(page_id);
        }
        self.highest_page_id += 1;
        Ok(self.highest_page_id)
    }

    fn get_page_type(&mut self, page_id: u64) -> Result<PageType> {
        let page_bytes = self.read_page(page_id)?;
        let page_header = Page::ref_from_prefix(page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: 0 })?
            .0;

        Ok(match page_header.page_type {
            1 => PageType::Meta,
            2 => PageType::FreeList,
            3 => PageType::Leaf,
            4 => PageType::Branch,
            _ => {
                return Err(BTreeError::CorruptPageType {
                    page_id,
                    raw_type: page_header.page_type,
                })
            }
        })
    }
}