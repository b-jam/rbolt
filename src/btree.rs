use crate::db::{Db, DbError};
use crate::page::{Page, PageType, BranchElement};
use std::collections::HashMap;
use std::fmt;
use zerocopy::FromBytes;

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
    db: &'a mut Db,
    root_page_id: u64,
    dirty_pages: HashMap<u64, Vec<u8>>,  // Modified pages (will be written on commit)
    free_list: Vec<u64>,
    highest_page_id: u64,
}

impl WriteTxn<'_> {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_recursive(self.root_page_id, key, value)
    }

    pub fn commit(self) -> Result<()> {
        // Write all dirty pages to disk and update the database header
        self.db.commit_dirty_pages(self.dirty_pages, self.highest_page_id)?;
        Ok(())
    }

    fn insert_recursive(&mut self, page_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let page_type = self.get_page_type(page_id)?;
        match page_type {
            PageType::Leaf => {
                self.insert_into_leaf(page_id, key, value)?;
            }
            PageType::Branch => {
                let child_page_id = self.find_child_page(page_id, key)?;
                self.insert_recursive(child_page_id, key, value)?;
            }
            _ => {
                return Err(BTreeError::InvalidPageType {
                    page_id,
                    page_type,
                });
            }
        }
        Ok(())
    }

    fn find_child_page(&mut self, page_id: u64, for_key: &[u8]) -> Result<u64> {
        let page_bytes = self.read_page(page_id)?;
        let (page_header, rest) = Page::ref_from_prefix(&page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: 0 })?;
        let element_count = page_header.count as usize;
        if element_count == 0 {
            return Err(BTreeError::EmptyBranchPage { page_id });
        }
        let elements = unsafe {
            let ptr = rest.as_ptr() as *const BranchElement;
            std::slice::from_raw_parts(ptr, element_count)
        };

        //TODO binary search
        // find the last element whose key <= search_key
        let mut child_page_id = elements[0].page_id;
        for element in elements.iter() {
            let key_data = &page_bytes[(element.kptr as usize)..(element.kptr + element.ksize) as usize];
            if for_key >= key_data {
                child_page_id = element.page_id;
            } else {
                break;
            }
        }
        Ok(child_page_id)
    }

    fn read_page(&self, page_id: u64) -> Result<Vec<u8>> {
        // could be already in dirty_pages
        if let Some(page_bytes) = self.dirty_pages.get(&page_id) {
            return Ok(page_bytes.clone());
        }
        // nah, read from mmap. let OS do page cache / page faults)
        Ok(self.db.read_page_bytes(page_id)?)
    }

    fn get_page_for_write(&mut self, page_id: u64) -> Result<&mut [u8]> {
        if !self.dirty_pages.contains_key(&page_id) {
            let page_bytes = self.read_page(page_id)?;
            self.dirty_pages.insert(page_id, page_bytes);
        }
        Ok(self.dirty_pages.get_mut(&page_id).unwrap())
    }

    // Insert a key-value pair into a leaf page
    fn insert_into_leaf(&mut self, _page_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let page_bytes = self.get_page_for_write(_page_id)?;
        let (_page_header, _rest) = Page::ref_from_prefix(page_bytes)
            .map_err(|_| BTreeError::CorruptPageType { page_id: _page_id, raw_type: 0 })?;

        // load into dirty pages
        // find insertion point (binary search)
        // check if page has room
        // if room: insert key-value pair
        // if no room: split_leaf()
        Ok(())
    }

    #[allow(dead_code)]
    fn split_leaf(&mut self, _page_id: u64) -> Result<()> {
        // Split leaf page into two leaf pages
        // first page contains ceil((m-1)/2)
        // second page contains rest
        // copy smallest search key value from second node to parent node (right bias)
        Ok(())
    }

    #[allow(dead_code)]
    fn split_branch(&mut self, _page_id: u64) -> Result<()> {
        // split the branch page into two branch pages
        // first page contains ceil(m/2)-1 keys
        // move the smallest key left to the parent node
        // second branch page contains the rest of the keys
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<u64> {
        // if free list is not empty, return first page id
        if let Some(page_id) = self.free_list.pop() {
            return Ok(page_id);
        }
        // else return highest_page_id + 1
        self.highest_page_id += 1;
        Ok(self.highest_page_id)
    }

    fn get_page_type(&mut self, page_id: u64) -> Result<PageType> {
        if let Some(page_bytes) = self.dirty_pages.get(&page_id) {
            // Already dirty page.
            let page_header = Page::ref_from_prefix(page_bytes)
                .map_err(|_| BTreeError::CorruptPageType { page_id, raw_type: 0 })?
                .0;
            return Ok(match page_header.page_type {
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
            });
        }

        let page = self.db.begin_read_transaction()?.get_page(page_id)?;
        Ok(match page.page_type {
            1 => PageType::Meta,
            2 => PageType::FreeList,
            3 => PageType::Leaf,
            4 => PageType::Branch,
            _ => {
                return Err(BTreeError::CorruptPageType {
                    page_id,
                    raw_type: page.page_type,
                })
            }
        })
    }
}