use crate::page::{Page, PageError, PageReader};
use std::fs::File;
use std::io::{self, Seek, Write};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard};
use memmap2::{MmapMut, MmapOptions};

pub const PAGE_SIZE: usize = 4096;
const MAGIC: u32 = 0x73796E63;
const VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct Header {
    magic: u32,
    version: u32,
    page_size: u32,

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
    pub fn get_page(&self, page_id: u64) -> Result<&'a Page, PageError> {
        self.mmap_guard.get_page(page_id, self.header.highest_page_id)
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
    pub fn open(path: &Path) -> io::Result<Self> {
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
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid database file magic num"));
        }

        Ok(Db {
            mmap: RwLock::new(initial_mmap),
            header,
            file,
        })

    }

    // Writes the header bytes to the beginning of the file using a standard Write operation.
    // This is used only for initial file creation.
    fn write_header(file: &mut File, header: &Header) -> io::Result<()> {
        // The header is written to the first page (Page 0)
        let header_bytes = unsafe {
            // Get a byte slice of the Header struct's memory
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

    fn read_header(mmap: &MmapMut) -> io::Result<Header> {
        if mmap.len() < std::mem::size_of::<Header>() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "File too small to contain header"));
        }
        let header_ref = unsafe {
            let ptr = mmap.as_ptr() as *const Header;
            &*ptr
        };
        Ok(*header_ref)
    }

    pub fn begin_read_transaction(&self) -> io::Result<ReadTxn<'_>> {
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
    pub fn commit_write_transaction(&self, new_data: &[u8]) -> io::Result<()> {
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


}