use crate::page::{Page, PageError, PageReader};
use std::fs::File;
use std::io::{self, Seek, Write};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard};
use std::fmt;
use memmap2::{MmapMut, MmapOptions};
use zerocopy::{FromBytes, Immutable, KnownLayout};

pub const PAGE_SIZE: usize = 4096;
const MAGIC: u32 = 0x73796E63;
const VERSION: u32 = 1;

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    Page(PageError),
    InvalidMagic { found: u32, expected: u32 },
    FileTooSmall { size: usize, required: usize },
    PageOutOfBounds { page_id: u64, file_size: usize },
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
#[derive(Clone, Copy, FromBytes, KnownLayout, Immutable)]
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


// Represents a read-only transaction on the database.
// It holds an immutable snapshot of the database file.
pub struct ReadTxn<'a> {
    // The read guard over the MmapMut. This ensures the underlying file
    // cannot be re-mapped until the transaction is dropped.
    mmap_guard: RwLockReadGuard<'a, MmapMut>,
    // A copy of the header used for this transaction's snapshot.
    header: Header,
}

impl<'a> ReadTxn<'a> {
    pub fn get_page(&self, page_id: u64) -> Result<&'a Page> {
        Ok(self.mmap_guard.get_page(page_id, self.header.highest_page_id)?)
    }
    pub fn root_page_id(&self) -> u64 {
        self.header.root_page_id
    }
}

pub struct Db {
    mmap: RwLock<MmapMut>,
    header: Header,
    file: File,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len() as usize;

        if file_len < PAGE_SIZE * 2 {
            // New database: initialize with minimim size
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
            header,
            file,
        })

    }

    // Writes the header bytes to the beginning of the file using a standard Write operation.
    // This is used only for initial file creation.
    fn write_header(file: &mut File, header: &Header) -> Result<()> {
        // The header is written to the first page (Page 0)
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                (header as *const Header) as *const u8,
                std::mem::size_of::<Header>(),
            )
        };

        // Rewind the file pointer to the start and write the header.
        file.seek(io::SeekFrom::Start(0))?;
        let mut writer = io::BufWriter::new(file);
        writer.write_all(header_bytes)?;
        writer.flush()?;

        // For initial setup, we don't need to sync the Mmap yet,
        // as the initial_mmap is created right after this.
        Ok(())
    }

    fn read_header(mmap: &MmapMut) -> Result<Header> {
        let header_size = std::mem::size_of::<Header>();
        if mmap.len() < header_size {
            return Err(DbError::FileTooSmall {
                size: mmap.len(),
                required: header_size,
            });
        }

        let header_bytes = &mmap[..header_size];
        let header = Header::ref_from_bytes(header_bytes)
            .map_err(|_| DbError::FileTooSmall {
                size: mmap.len(),
                required: header_size,
            })?;
        Ok(*header)
    }

    pub fn begin_read_transaction(&self) -> Result<ReadTxn<'_>> {
        // Acquire a read lock on the Mmap. This ensures no write transaction
        // is currently trying to update the Mmap reference.
        let mmap_guard = self.mmap.read().unwrap();
        // Use the *current* header as the snapshot header.
        let header = self.header;
        println!("   [OK] Read transaction started on database of size {} bytes.", mmap_guard.len());
        Ok(ReadTxn {
            mmap_guard,
            header,
        })
    }

    pub fn highest_page_id(&self) -> u64 {
        self.header.highest_page_id
    }

    pub fn root_page_id(&self) -> u64 {
        self.header.root_page_id
    }

    pub fn read_page_bytes(&self, page_id: u64) -> Result<Vec<u8>> {
        let mmap = self.mmap.read().unwrap();

        let offset = page_id as usize * PAGE_SIZE;
        if offset + PAGE_SIZE > mmap.len() {
            return Err(DbError::PageOutOfBounds {
                page_id,
                file_size: mmap.len(),
            });
        }

        // We do have to copy page data to return without a lock
        let mut page_bytes = vec![0u8; PAGE_SIZE];
        page_bytes.copy_from_slice(&mmap[offset..offset + PAGE_SIZE]);
        Ok(page_bytes)
    }

    pub fn commit_write_transaction(&self, new_data: &[u8]) -> Result<()> {
        // Acquite an exclusive write lock on Mmap. This blocks all new readers
        // and any currently waiting writers.
        let mut mmap_guard = self.mmap.write().unwrap();

        self.file.set_len(new_data.len() as u64)?;
        let new_mmap = unsafe { MmapMut::map_mut(&self.file)? };
        // copy on write
        *mmap_guard = new_mmap;

        mmap_guard.flush()?;
        println!("   [OK] Write transaction committed and Mmap updated and flushed.");
        Ok(())
    }

    pub fn commit_dirty_pages(
        &mut self,
        dirty_pages: std::collections::HashMap<u64, Vec<u8>>,
        new_highest_page_id: u64,
    ) -> Result<()> {
        // grab write lock
        let mut mmap = self.mmap.write().unwrap();

        // do we need to increase the file?
        let required_size = (new_highest_page_id as usize + 1) * PAGE_SIZE;
        if required_size > mmap.len() {
            // yes, replace mmap with bigger one.
            self.file.set_len(required_size as u64)?;
            let new_mmap = unsafe { MmapMut::map_mut(&self.file)? };
            *mmap = new_mmap;  // old MmapMut is dropped here
        }

        // Write dirty pages, copy on write
        for (page_id, page_bytes) in dirty_pages.iter() {
            let offset = *page_id as usize * PAGE_SIZE;
            if offset + PAGE_SIZE <= mmap.len() {
                mmap[offset..offset + PAGE_SIZE].copy_from_slice(page_bytes);
            }
        }

        self.header.highest_page_id = new_highest_page_id;
        self.header.tx_id += 1;

        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                (&self.header as *const Header) as *const u8,
                std::mem::size_of::<Header>(),
            )
        };
        mmap[..header_bytes.len()].copy_from_slice(header_bytes);

        // Flush dirty pages to disk
        mmap.flush()?;

        println!("   [OK] Committed {} dirty pages, tx_id={}", dirty_pages.len(), self.header.tx_id);
        Ok(())
    }


}