use crate::db::PAGE_SIZE;
use std::mem;
use std::sync::RwLockReadGuard;
use memmap2::MmapMut;
use zerocopy::{FromBytes, Immutable, KnownLayout};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageError {
    OutOfBounds { page_id: u64, mmap_size: usize },
    InvalidPageId { page_id: u64, highest_page_id: u64 },
}

impl From<PageError> for std::io::Error {
    fn from(err: PageError) -> Self {
        match err {
            PageError::OutOfBounds { page_id, mmap_size } => {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Page {} out of bounds (mmap size: {})", page_id, mmap_size)
                )
            }
            PageError::InvalidPageId { page_id, highest_page_id } => {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid page ID {} (highest: {})", page_id, highest_page_id)
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PageType {
    Meta = 1, //txinfo, root page
    FreeList = 2, //pages that have been freed and can be reused
    Leaf = 3, //contains actual KV
    Branch = 4, //internal nodes of B tree. key or key range, page id
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct Page {
    pub id: u64, // 8 bytes, 2^64 very large
    pub page_type: u8, // 1 byte, mapped to PageType
    pub _padding: u8, // 1 byte of explicit padding
    pub count: u16, // The number of kv or child pointers, 2^16 = 65535
    pub overflow: u32, // overflow multiple pages, 2^32 = 4294967296
}

const PAGE_HEADER_SIZE: usize = mem::size_of::<Page>(); // 16 bytes

#[repr(C)]
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct BranchElement {
    pub page_id: u64, // 8 bytes, the ID of the child page this element points to.
    pub ksize: u16, // Size of the key, 2^16 = 65535
    pub kptr: u16, // Offset to the key data within the page
    pub _padding: [u8; 4], // Explicit padding to 16 bytes
}

#[repr(C)]
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable)]
pub struct LeafElement {
    pub ksize: u16,
    pub vsize: u16,
    pub kptr: u16,
    pub vptr: u16,
}



pub trait PageReader {
    // Retrieves a Page struct reference from a given page ID within the memory map.
    // Validates both physical bounds (mmap size) and logical validity (page_id <= highest_page_id).
    // Returns an error if the page_id is invalid.
    fn get_page(&self, page_id: u64, highest_page_id: u64) -> Result<&'static Page, PageError>;
}

impl PageReader for MmapMut {
    fn get_page(&self, page_id: u64, highest_page_id: u64) -> Result<&'static Page, PageError> {
        // Logical validation
        if page_id > highest_page_id {
            return Err(PageError::InvalidPageId {
                page_id,
                highest_page_id,
            });
        }

        // Physical validation
        let offset = page_id as usize * PAGE_SIZE;
        if offset + PAGE_HEADER_SIZE > self.len() {
            return Err(PageError::OutOfBounds {
                page_id,
                mmap_size: self.len(),
            });
        }

        // zero-copy, cast raw pointer to Page reference
        unsafe {
            let ptr = self.as_ptr().add(offset) as *const Page;
            // reference lifetime to 'self' MmapMut reference, (tx RwLock)
            Ok(&*ptr)
        }
    }
}

impl<'a> PageReader for RwLockReadGuard<'a, MmapMut> {
    fn get_page(&self, page_id: u64, highest_page_id: u64) -> Result<&'static Page, PageError> {
        // Delegate to the MmapMut implementation
        (**self).get_page(page_id, highest_page_id)
    }
}
