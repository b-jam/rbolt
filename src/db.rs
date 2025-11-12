use crate::page::{BRANCH_ELEMENT_SIZE, BranchElement, LEAF_ELEMENT_SIZE, LeafElement, PAGE_HEADER_SIZE, Page, PageError, PageReader, PageType};
use crate::search;
use std::fs::File;
use std::io::{self, Seek, Write};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, Mutex};
use std::cell::UnsafeCell;
use std::fmt;
use memmap2::{MmapMut, MmapOptions};
use zerocopy::{FromBytes, IntoBytes, Immutable, KnownLayout};

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();
const MAGIC: u32 = 0x73796E63;
const VERSION: u32 = 1;

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    Page(PageError),
    InvalidMagic { found: u32, expected: u32 },
    FileTooSmall { size: usize, required: usize },
    PageOutOfBounds { page_id: u64, file_size: usize },
    PageFormat,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(err) => write!(f, "IO error: {}", err),
            DbError::Page(err) => write!(f, "Page error: {:?}", err),
            DbError::InvalidMagic { found, expected } => {
                write!(f, "Invalid magic number: found 0x{:x}, expected 0x{:x}", found, expected)
            }
            DbError::FileTooSmall { size, required } => {
                write!(f, "File too small: {} bytes, required {} bytes", size, required)
            }
            DbError::PageOutOfBounds { page_id, file_size } => {
                write!(f, "Page {} out of bounds (file size: {})", page_id, file_size)
            }
            DbError::PageFormat => {
                write!(f, "Failed to parse page structure")
            }
        }
    }
}

impl std::error::Error for DbError {}

impl From<io::Error> for DbError {
    fn from(err: io::Error) -> Self {
        DbError::Io(err)
    }
}

impl From<PageError> for DbError {
    fn from(err: PageError) -> Self {
        DbError::Page(err)
    }
}

type Result<T> = std::result::Result<T, DbError>;

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
struct Header {
    magic: u32,
    version: u32,
    page_size: u32,
    _padding: u32,  // Explicit padding to align to 8 bytes

    root_page_id: u64, // Location of Root Page. always 0 but u64 for consistent sizing
    free_list_page_id: u64, //Location of the Free List Page. always 1 but u64 for consistent sizing

    highest_page_id: u64, //highest allocated page ID
    tx_id: u64, //transaction id
}


impl Header {
    fn new(page_size: u32) -> Self {
        Header {
            magic: MAGIC,
            version: VERSION,
            page_size,
            _padding: 0,
            root_page_id: 0,
            free_list_page_id: 1, // Free list on page 1
            highest_page_id: 2,   // Highest allocated page ID - start at 2
            tx_id: 0,
        }
    }
}


pub struct ReadTxn<'a> {
    mmap_guard: RwLockReadGuard<'a, MmapMut>,
    header: Header,
}

impl<'a> ReadTxn<'a> {
    pub fn get_page(&self, page_id: u64) -> Result<&'a Page> {
        Ok(self.mmap_guard.get_page(page_id, self.header.highest_page_id)?)
    }
    pub fn root_page_id(&self) -> u64 {
        self.header.root_page_id
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_recursive(self.header.root_page_id, key)
    }

    fn get_recursive(&self, page_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let page = self.get_page(page_id)?;

        match page.page_type {
            t if t == PageType::Leaf as u8 => {
                self.search_leaf(page_id, key)
            }
            t if t == PageType::Branch as u8 => {
                let child_id = self.find_child_in_branch(page_id, key)?;
                self.get_recursive(child_id, key)
            }
            _ => Ok(None),
        }
    }

    fn search_leaf(&self, page_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let page_offset = page_id as usize * PAGE_SIZE;
        let page_bytes = &self.mmap_guard[page_offset..page_offset + PAGE_SIZE];
        let (page, page_body) = Page::ref_from_prefix(page_bytes)
            .map_err(|_| DbError::PageFormat)?;

        let element_count = page.count as usize;
        let (index, found) = search::search_leaf_elements(page_body, element_count, key)
            .map_err(|_| DbError::PageFormat)?;

        if found {
            let elem_bytes = &page_body[index*LEAF_ELEMENT_SIZE..(index+1)*LEAF_ELEMENT_SIZE];
            let elem = LeafElement::ref_from_bytes(elem_bytes)
                .map_err(|_| DbError::PageFormat)?;
            let value = &page_body[elem.vptr as usize..(elem.vptr + elem.vsize) as usize];
            Ok(Some(value.to_vec()))
        } else {
            Ok(None)
        }
    }

    fn find_child_in_branch(&self, page_id: u64, for_key: &[u8]) -> Result<u64> {
        let page_offset = page_id as usize * PAGE_SIZE;
        let page_bytes = &self.mmap_guard[page_offset..page_offset + PAGE_SIZE];
        let (page, page_body) = Page::ref_from_prefix(page_bytes)
            .map_err(|_| DbError::PageFormat)?;

        let element_count = page.count as usize;
        let (result_index, found) = search::search_branch_elements(page_body, element_count, for_key)
            .map_err(|_| DbError::PageFormat)?;

        let child_index = if found {
            result_index
        } else  {
            result_index.saturating_sub(1)
        };

        let elem_bytes = &page_body[child_index*BRANCH_ELEMENT_SIZE..(child_index+1)*BRANCH_ELEMENT_SIZE];
        let elem = BranchElement::ref_from_bytes(elem_bytes)
            .map_err(|_| DbError::PageFormat)?;

        Ok(elem.page_id)
    }
}

pub struct Db {
    mmap: RwLock<MmapMut>,
    write_lock: Mutex<()>,
    header: RwLock<Header>,
    file: UnsafeCell<File>,
}

// Db can be safely sent between threads
// - mmap: RwLock
// - write_lock: Mutex
// - header: RwLock
// - file: written while holding mmap write lock
unsafe impl Send for Db {}
unsafe impl Sync for Db {}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len() as usize;

        if file_len < PAGE_SIZE * 2 {
            file.set_len(PAGE_SIZE as u64 * 2)?;

            let default_header = Header::new(PAGE_SIZE as u32);
            Self::write_header(&mut file, &default_header)?;
        }

        let initial_mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .expect("Failed to create mutable memory map")
        };

        let header = Self::read_header(&initial_mmap)?;

        if header.magic != MAGIC {
            return Err(DbError::InvalidMagic {
                found: header.magic,
                expected: MAGIC,
            });
        }

        Ok(Db {
            mmap: RwLock::new(initial_mmap),
            write_lock: Mutex::new(()),
            header: RwLock::new(header),
            file: UnsafeCell::new(file),
        })

    }

    fn write_header(file: &mut File, header: &Header) -> Result<()> {
        file.seek(io::SeekFrom::Start(0))?;
        let mut writer = io::BufWriter::new(file);
        writer.write_all(header.as_bytes())?;
        writer.flush()?;
        Ok(())
    }

    fn read_header(mmap: &MmapMut) -> Result<Header> {
        if mmap.len() < HEADER_SIZE {
            return Err(DbError::FileTooSmall {
                size: mmap.len(),
                required: HEADER_SIZE,
            });
        }

        let header_bytes = &mmap[..HEADER_SIZE];
        let header = Header::ref_from_bytes(header_bytes)
            .map_err(|_| DbError::FileTooSmall {
                size: mmap.len(),
                required: HEADER_SIZE,
            })?;
        Ok(*header)
    }

    pub fn begin_read_transaction(&self) -> Result<ReadTxn<'_>> {
        let mmap_guard = self.mmap.read().unwrap();
        let header = *self.header.read().unwrap();
        println!("   [OK] Read transaction started on database of size {} bytes.", mmap_guard.len());
        Ok(ReadTxn {
            mmap_guard,
            header,
        })
    }

    pub fn commit(&self, dirty_pages: std::collections::HashMap<u64, Vec<u8>>, highest_page_id: u64, root_page_id: u64) -> Result<()> {
        self.commit_dirty_pages(dirty_pages, highest_page_id, root_page_id)?;
        Ok(())
    }

    pub fn begin_write_transaction(&self) -> Result<crate::btree::WriteTxn<'_>> {
        let write_guard = self.write_lock.lock().unwrap();
        let needs_init = {
            let mmap = self.mmap.read().unwrap();
            let root_offset = 2 * PAGE_SIZE;
            root_offset >= mmap.len() || mmap[root_offset] == 0
        };

        if needs_init {
            self.initialize_root_page()?;
        }

        let (root_page_id, highest_page_id) = {
            let header = self.header.read().unwrap();
            (header.root_page_id, header.highest_page_id)
        };
        let free_list = Vec::new();

        let mmap_guard = self.mmap.read().unwrap();

        Ok(crate::btree::WriteTxn::new(
            write_guard,
            mmap_guard,
            root_page_id,
            free_list,
            highest_page_id,
        ))
    }

    fn initialize_root_page(&self) -> Result<()> {
        let mut mmap = self.mmap.write().unwrap();
        let required_size = 3 * PAGE_SIZE; // 0, 1, 2
        if mmap.len() < required_size {
            unsafe {
                let file = &mut *self.file.get();
                file.set_len(required_size as u64)?;
                let new_mmap = MmapMut::map_mut(&*file)?;
                *mmap = new_mmap;
            }
        }

        let root_offset = 2 * PAGE_SIZE;
        let page_bytes = &mut mmap[root_offset..root_offset + PAGE_SIZE];

        let page = Page {
            id: 2,
            page_type: PageType::Leaf as u8,
            _padding: 0,
            count: 0,
            overflow: 0,
        };

        page_bytes[..PAGE_HEADER_SIZE].copy_from_slice(page.as_bytes());
        page_bytes[PAGE_HEADER_SIZE..].fill(0);

        let mut header = self.header.write().unwrap();
        header.root_page_id = 2;
        header.highest_page_id = 2;

        mmap[..HEADER_SIZE].copy_from_slice(header.as_bytes());
        mmap.flush()?;

        println!("   [OK] Initialized root page (page 2) as empty leaf");
        Ok(())
    }

    pub fn commit_write_transaction(&self, new_data: &[u8]) -> Result<()> {
        let mut mmap_guard = self.mmap.write().unwrap();
        unsafe {
            let file = &mut *self.file.get();
            file.set_len(new_data.len() as u64)?;
            let new_mmap = MmapMut::map_mut(&*file)?;
            *mmap_guard = new_mmap;
        }

        mmap_guard.flush()?;
        println!("   [OK] Write transaction committed and Mmap updated and flushed.");
        Ok(())
    }

    pub fn commit_dirty_pages(
        &self,
        dirty_pages: std::collections::HashMap<u64, Vec<u8>>,
        new_highest_page_id: u64,
        new_root_page_id: u64,
    ) -> Result<()> {
        let mut mmap = self.mmap.write().unwrap();

        let required_size = (new_highest_page_id as usize + 1) * PAGE_SIZE;
        if required_size > mmap.len() {
            unsafe {
                let file = &mut *self.file.get();
                file.set_len(required_size as u64)?;
                let new_mmap = MmapMut::map_mut(&*file)?;
                *mmap = new_mmap;
            }
        }

        for (page_id, page_bytes) in dirty_pages.iter() {
            let offset = *page_id as usize * PAGE_SIZE;
            if offset + PAGE_SIZE <= mmap.len() {
                mmap[offset..offset + PAGE_SIZE].copy_from_slice(page_bytes);
            }
        }

        let mut header = self.header.write().unwrap();
        header.highest_page_id = new_highest_page_id;
        header.root_page_id = new_root_page_id;
        header.tx_id += 1;

        mmap[..std::mem::size_of::<Header>()].copy_from_slice(header.as_bytes());

        mmap.flush()?;

        println!("   [OK] Committed {} dirty pages, tx_id={}", dirty_pages.len(), header.tx_id);
        Ok(())
    }


}